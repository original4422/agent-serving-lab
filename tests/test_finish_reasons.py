"""Finish reasons are observations, independent of transport success."""
from contextlib import asynccontextmanager, redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import httpx

from serving_lab.backend import OpenAIBackend
from serving_lab.cli import experiment
from serving_lab.metrics import summarize
from serving_lab.scheduler import run
from serving_lab.workload import generate
from test_reporting import arguments, completed


def sse(events, done=True):
    return ''.join('data: ' + json.dumps(event) + '\n\n' for event in events) + ('data: [DONE]\n\n' if done else '')


def ending(reason):
    return [{'choices': [{'delta': {'content': 'x'}, 'finish_reason': None}]},
            {'choices': [{'delta': {}, 'finish_reason': reason}]},
            {'choices': [], 'usage': {'completion_tokens': 1}}]


@asynccontextmanager
async def backend_for(handler):
    backend = OpenAIBackend('http://fixture.invalid/v1', 'fixture')
    await backend.close()
    backend.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        yield backend
    finally:
        await backend.close()


class FinishReasonTests(unittest.IsolatedAsyncioTestCase):
    async def test_distinct_and_unknown_reasons_without_content_in_terminal_chunk(self):
        for reason in ('stop', 'length', 'tool_calls', 'provider_custom'):
            with self.subTest(reason=reason):
                async with backend_for(lambda _: httpx.Response(200, text=sse(ending(reason)))) as backend:
                    request = generate(count=1)['requests'][0]
                    request['max_tokens'] = 1
                    result = await backend.stream(request)
                self.assertEqual(result['finish_reason'], reason)
                self.assertIsNone(result['error'])
                self.assertEqual(len(result['chunk_times_s']), 1)
                self.assertEqual(result['usage'], {'completion_tokens': 1})

    async def test_last_nonnull_first_choice_and_missing_are_not_inferred_from_usage(self):
        events = ending('stop')[:2] + [
            {'choices': [{'delta': {}, 'finish_reason': None}]},
            {'choices': [{'delta': {}}]},
            {'choices': [{'finish_reason': 'length'}, {'finish_reason': 'tool_calls'}]},
            {'choices': [{'finish_reason': None}]},
            {'choices': [], 'usage': {'completion_tokens': 1}},
        ]
        missing = [{'choices': [{'delta': {'content': 'x'}}]},
                   {'choices': [], 'usage': {'completion_tokens': 1}}]
        for chunks, expected in ((events, 'length'), (ending(None), None), (missing, None)):
            async with backend_for(lambda _: httpx.Response(200, text=sse(chunks))) as backend:
                request = generate(count=1)['requests'][0]
                request['max_tokens'] = 1
                result = await backend.stream(request)
            self.assertEqual(result['finish_reason'], expected)
            self.assertIsNone(result['error'])

    async def test_observed_reason_survives_subsequent_transport_failure(self):
        class BrokenStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield sse(ending('length'), done=False).encode()
                raise httpx.ReadError('fixture disconnect')
        async with backend_for(lambda _: httpx.Response(200, stream=BrokenStream())) as backend:
            records, elapsed = await run(generate(count=1), backend)
        self.assertEqual(records[0]['finish_reason'], 'length')
        self.assertEqual(records[0]['status'], 'failed')
        self.assertEqual(records[0]['error'], 'ReadError')
        metrics = summarize(records, elapsed, 1)
        self.assertEqual(metrics['finish_reason_counts'], {})
        self.assertEqual(metrics['finish_reason_missing'], 0)

    def test_only_success_counts_include_missing_old_records(self):
        record = completed()[0][0]
        records = [{**record, 'finish_reason': 'stop'}, {**record, 'finish_reason': 'length'},
                   {**record, 'finish_reason': None}, record,
                   {**record, 'finish_reason': 'length', 'status': 'failed'},
                   {**record, 'finish_reason': 'stop', 'status': 'expired'},
                   {'status': 'blocked'}]
        metrics = summarize(records, 1, 1)
        self.assertEqual(metrics['finish_reason_counts'], {'stop': 1, 'length': 1})
        self.assertEqual(metrics['finish_reason_missing'], 2)
        self.assertEqual(sum(metrics['finish_reason_counts'].values()) + metrics['finish_reason_missing'], metrics['succeeded'])
        self.assertEqual(metrics['output_tokens_per_s'], 4)

    async def test_cli_json_markdown_by_kind_and_success_exit_remain_consistent(self):
        reasons = iter(('stop', 'length', 'tool_calls', 'provider|custom'))
        async with backend_for(lambda _: httpx.Response(200, text=sse(ending(next(reasons))))) as backend:
            with TemporaryDirectory() as directory:
                workload = generate(count=4)
                for request in workload['requests']:
                    request.update(arrival_s=0, tool_delay_s=0)
                source = Path(directory) / 'input.json'
                source.write_text(json.dumps(workload))
                args = arguments(str(Path(directory) / 'output'))
                args.workload, args.repeats = str(source), 1
                with patch('serving_lab.cli.OpenAIBackend', return_value=backend), redirect_stdout(io.StringIO()):
                    code = await experiment(args, 'http://fixture.invalid/v1', 'scripted_http_not_llm')
                report = json.loads((Path(args.output) / 'report.json').read_text())
                text = (Path(args.output) / 'report.md').read_text()
        result = report['runs'][0]
        self.assertEqual(code, 0)
        self.assertEqual(result['metrics']['succeeded'], 4)
        self.assertEqual(result['metrics']['finish_reason_counts'], {'stop': 1, 'length': 1, 'tool_calls': 1, 'provider|custom': 1})
        self.assertEqual(result['metrics']['finish_reason_missing'], 0)
        self.assertTrue(all(record['status'] == 'ok' for record in result['requests']))
        self.assertEqual(sum(sum(m['finish_reason_counts'].values()) for m in result['by_kind'].values()), 4)
        self.assertIn('## Successful-stream finish reasons', text)
        self.assertIn('length', text)
        self.assertIn('provider&#124;custom', text)

"""Owned output files and atomic snapshots of completed experiment rounds."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile

from .metrics import markdown


class Output:
    def __init__(self, directory=None):
        if directory is None:
            root = Path("results")
            root.mkdir(exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            self.path = Path(tempfile.mkdtemp(prefix=f"run-{stamp}-", dir=root))
        else:
            self.path = Path(directory)
            self.path.mkdir(parents=True, exist_ok=True)
        self.files = {}
        try:
            for name in ("workload.json", "report.json", "report.md"):
                target = self.path / name
                with target.open("x", encoding="utf-8"):
                    pass
                self.files[name] = target
        except FileExistsError:
            for target in self.files.values():
                target.unlink()
            raise FileExistsError(f"Output already contains a result file; choose a new --output directory: {self.path}") from None

    def write(self, name, content):
        target = self.files[name]
        descriptor, temporary = tempfile.mkstemp(prefix=f".{name}-", dir=self.path)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def workload(self, workload):
        self.write("workload.json", json.dumps(workload, indent=2) + "\n")

    def snapshot(self, report):
        report["completed_runs"] = len(report["runs"])
        # JSON is authoritative; Markdown may lag if the process is interrupted.
        self.write("report.json", json.dumps(report, indent=2) + "\n")
        self.write("report.md", markdown(report))

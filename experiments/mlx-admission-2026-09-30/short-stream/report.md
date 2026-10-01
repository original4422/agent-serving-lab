# Agent Serving Lab

Evidence: **external_backend_unverified**

Client admission only; no server scheduling or engine changes.

| Repeat | Policy | OK / failed / blocked | Queue p95 (s) | TTFT p95 (s) | E2E p95 (s) | requests/s |
|---:|---|---:|---:|---:|---:|---:|
| 1 | shortest-input | 16 / 0 / 0 | 4.7754 | 5.9178 | 6.5184 | 2.4359 |
| 1 | fcfs | 16 / 0 / 0 | 4.5553 | 4.8271 | 5.2480 | 2.4359 |
| 1 | aging | 16 / 0 / 0 | 4.5249 | 4.7949 | 5.2223 | 2.4455 |
| 2 | aging | 16 / 0 / 0 | 4.5307 | 4.8014 | 5.2233 | 2.4451 |
| 2 | shortest-input | 16 / 0 / 0 | 5.4216 | 6.5995 | 7.2045 | 2.2055 |
| 2 | fcfs | 16 / 0 / 0 | 5.3078 | 5.9665 | 6.3829 | 2.0453 |

## Per-kind waiting and completion

| Repeat | Policy | Kind | OK / failed / blocked | Queue mean / p95 / max (s) | E2E mean / p95 / max (s) |
|---:|---|---|---:|---:|---:|
| 1 | shortest-input | long | 2 / 0 / 0 | 4.7751 / 4.7754 / 4.7754 | 6.5180 / 6.5184 / 6.5184 |
| 1 | shortest-input | short | 14 / 0 / 0 | 1.3957 / 3.5096 / 3.5096 | 2.0850 / 4.2248 / 4.2248 |
| 1 | fcfs | long | 2 / 0 / 0 | 0.6271 / 0.6274 / 0.6274 | 2.3805 / 2.3809 / 2.3809 |
| 1 | fcfs | short | 14 / 0 / 0 | 2.8873 / 4.5553 / 4.5553 | 3.5751 / 5.2480 / 5.2480 |
| 1 | aging | long | 2 / 0 / 0 | 1.3014 / 1.3017 / 1.3017 | 3.0512 / 3.0517 / 3.0517 |
| 1 | aging | short | 14 / 0 / 0 | 2.6227 / 4.5249 / 4.5249 | 3.3074 / 5.2223 / 5.2223 |
| 2 | aging | long | 2 / 0 / 0 | 1.3100 / 1.3102 / 1.3102 | 3.0766 / 3.0770 / 3.0770 |
| 2 | aging | short | 14 / 0 / 0 | 2.6374 / 4.5307 / 4.5307 | 3.3198 / 5.2233 / 5.2233 |
| 2 | shortest-input | long | 2 / 0 / 0 | 5.4213 / 5.4216 / 5.4216 | 7.2042 / 7.2045 / 7.2045 |
| 2 | shortest-input | short | 14 / 0 / 0 | 1.5830 / 4.1305 / 4.1305 | 2.3646 / 4.8709 / 4.8709 |
| 2 | fcfs | long | 2 / 0 / 0 | 0.6574 / 0.6577 / 0.6577 | 2.4357 / 2.4360 / 2.4360 |
| 2 | fcfs | short | 14 / 0 / 0 | 3.0234 / 5.3078 / 5.3078 | 3.8472 / 6.3829 / 6.3829 |

Full per-request timing, token coverage, chunk intervals and starvation-threshold counts are in JSON.
Policies share the same workload; dependency release times depend on parent completion.

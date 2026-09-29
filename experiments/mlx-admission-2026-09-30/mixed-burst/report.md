# Agent Serving Lab

Evidence: **external_backend_unverified**

Client admission only; no server scheduling or engine changes.

| Repeat | Policy | OK / failed / blocked | Queue p95 (s) | TTFT p95 (s) | E2E p95 (s) | requests/s |
|---:|---|---:|---:|---:|---:|---:|
| 1 | shortest-input | 16 / 0 / 0 | 5.6464 | 7.0756 | 7.7121 | 2.0747 |
| 1 | fcfs | 16 / 0 / 0 | 8.5308 | 8.8054 | 9.2358 | 1.7324 |
| 1 | aging | 16 / 0 / 0 | 8.5799 | 8.8498 | 9.2670 | 1.7266 |
| 2 | aging | 16 / 0 / 0 | 9.2926 | 9.4424 | 9.8160 | 1.6298 |
| 2 | shortest-input | 16 / 0 / 0 | 8.1017 | 9.2334 | 9.7032 | 1.6489 |
| 2 | fcfs | 16 / 0 / 0 | 8.6074 | 8.8806 | 9.2975 | 1.7209 |

## Per-kind waiting and completion

| Repeat | Policy | Kind | OK / failed / blocked | Queue mean / p95 / max (s) | E2E mean / p95 / max (s) |
|---:|---|---|---:|---:|---:|
| 1 | shortest-input | long | 4 / 0 / 0 | 4.8399 / 5.6464 / 5.6464 | 6.6789 / 7.7121 / 7.7121 |
| 1 | shortest-input | short | 12 / 0 / 0 | 1.6901 / 3.3636 / 3.3636 | 2.3624 / 4.0364 / 4.0364 |
| 1 | fcfs | long | 4 / 0 / 0 | 3.4046 / 6.9598 / 6.9598 | 5.0296 / 8.5301 / 8.5301 |
| 1 | fcfs | short | 12 / 0 / 0 | 4.4882 / 8.5308 / 8.5308 | 5.4857 / 9.2358 / 9.2358 |
| 1 | aging | long | 4 / 0 / 0 | 4.0485 / 6.9469 / 6.9469 | 5.6952 / 8.5799 / 8.5799 |
| 1 | aging | short | 12 / 0 / 0 | 3.6292 / 8.5799 / 8.5799 | 4.6248 / 9.2670 / 9.2670 |
| 2 | aging | long | 4 / 0 / 0 | 4.4154 / 7.6586 / 7.6586 | 6.2604 / 9.2926 / 9.2926 |
| 2 | aging | short | 12 / 0 / 0 | 3.5910 / 9.2926 / 9.2926 | 4.5684 / 9.8160 / 9.8160 |
| 2 | shortest-input | long | 4 / 0 / 0 | 6.6106 / 8.1017 / 8.1017 | 8.6025 / 9.7032 / 9.7032 |
| 2 | shortest-input | short | 12 / 0 / 0 | 2.0109 / 4.6551 / 4.6551 | 2.9215 / 5.7423 / 5.7423 |
| 2 | fcfs | long | 4 / 0 / 0 | 3.4135 / 6.9982 / 6.9982 | 5.0602 / 8.6068 / 8.6068 |
| 2 | fcfs | short | 12 / 0 / 0 | 4.5118 / 8.6074 / 8.6074 | 5.5124 / 9.2975 / 9.2975 |

Full per-request timing, token coverage, chunk intervals and starvation-threshold counts are in JSON.
Policies share the same workload; dependency release times depend on parent completion.

# Phase 4 benchmark report

## Scope

This is a deterministic application-layer simulation, not a network-latency benchmark.
It uses six agents, five dependent tasks, and ten fixed events. Event E7 removes
`coder` and reassigns implementation work to `coder-backup`.

## Primary results

| Metric | Full-mesh baseline | Optimized strategy | Reduction |
|---|---:|---:|---:|
| Messages | 42 | 6 | 85.7% |
| Application payload (bytes) | 24,212 | 2,668 | 89.0% |
| Estimated LLM tokens | 1,681 | 181 | 89.2% |
| Mean simulated latency (ms) | 18.634 | 18.837 | N/A |
| P95 simulated latency (ms) | 29.412 | 14.74 | N/A |
| Key-event preservation | 100% | 100% | Must remain 100% |
| Mean active task edges | 26 | 5 | — |

## How to read the figures

1. **traffic_comparison.svg** contains three independent small multiples. Do not
   compare bar heights across panels; compare baseline and optimized bars within
   the same metric only.
2. **topology_timeline.svg** separates the full-mesh and optimized edge counts
   into two panels. This prevents the smaller optimized topology from being
   visually flattened by the full-mesh scale. The red marker is event E7.
3. **policy_outcomes.svg** reports only policy-decision counts. It intentionally
   does not repeat traffic or topology conclusions already shown elsewhere.

## Policy and topology evidence

- Policy counts: `{'SEND': 6, 'DEFER': 1, 'MERGE': 1, 'DROP': 2}`.
- Key events delivered: 3/3.
- Topology rebuilds: 10; edge changes: 9.
- Mean local reconstruction scope: 3.5 agents.

The three SVG files are intended as figures; methodological explanation and
numerical interpretation remain in this report to avoid mixing long prose into charts.

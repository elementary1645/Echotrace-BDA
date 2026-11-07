# EchoTrace

**PANNs hears. EchoTrace connects.**

Open-weight, fully local AI that turns environmental sound into situational awareness for
emergency responders, industrial-safety teams and smart-building operators when cameras can't see
(smoke, darkness, blind spots). It detects acoustic events with PANNs CNN14, places each one
approximately LEFT / CENTRE / RIGHT with GCC-PHAT on a 2-mic array, correlates events into
sequences, and raises explainable GREEN / AMBER / RED alerts. It complements CCTV; it never replaces it.

> Work in progress (Hack Day build). Full documentation lands in the final phase.

## Quick setup (Windows, PowerShell)

```powershell
py -3.12 -m venv backend\.venv
backend\.venv\Scripts\python.exe -m pip install torch --index-url https://download.pytorch.org/whl/cu128
backend\.venv\Scripts\python.exe -m pip install -r backend\requirements.txt
backend\.venv\Scripts\python.exe backend\scripts\download_weights.py
backend\.venv\Scripts\python.exe backend\scripts\list_devices.py
```

## Licence and attribution

Code: MIT. Model weights: PANNs CNN14 (Kong et al., 2020), CC BY 4.0, downloaded from Zenodo; not included in this repo.

Kafka analytics integration, schema semantics, setup, tests, and delivery limits:
[BDA Phase 1 guide](bda/README.md).

Phase 2 streaming analytics: [Spark setup, recovery, tests and demonstrations](bda/PHASE2.md).

Phase 3: [historical Spark SQL analytics and reproducible benchmarks](bda/PHASE3.md),
with [executed validation and measured results](bda/PHASE3_VALIDATION.md).

Phase 4: [Analytics Command Center architecture and limitations](bda/PHASE4.md),
[professor demonstration](bda/analytics/DEMO.md), [analytics API](bda/analytics/API.md),
and [executed validation](bda/PHASE4_VALIDATION.md).

Weight-free dashboard demonstration (Python, Node and audio system libraries required):

```bash
infrastructure/dashboard/run.sh setup
infrastructure/dashboard/run.sh serve
# Open http://127.0.0.1:8000/
```

Phase 5: [integration, reliability and measured results](bda/PHASE5.md),
[executed validation](bda/PHASE5_VALIDATION.md), and
[reproduction, preflight and operational safety](bda/reliability/OPERATIONS.md).

```bash
infrastructure/reliability/run.sh preflight
infrastructure/reliability/run.sh verify artifacts/phase5/evaluator-001 --campaign --port 19096
```

This uses an isolated disposable broker and new output/checkpoint roots; existing
application infrastructure and benchmark evidence remain separate.

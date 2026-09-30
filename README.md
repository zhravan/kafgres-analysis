# Kafka vs Kafgres Benchmark

Independent, reproducible performance benchmark comparing Apache Kafka with Kafgres.

The benchmark uses the standard Kafka protocol client for both systems and contains no Kafgres-specific client path.

## Scope

- producer throughput
- consumer throughput
- end-to-end latency
- p50 / p95 / p99 / p99.9
- CPU, memory, disk and network usage
- message-size sensitivity
- producer/consumer concurrency
- partition scaling
- sustained throughput
- restart/recovery

Initial versions:

- Kafgres: \`00168534b8899300896b5ca1582a6ecca3de81d1\` (0.2.0 line)
- Apache Kafka: 4.3.1

See \`docs/methodology.md\` for the fairness rules and matrix.

## Quick start

\`\`\`bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

./scripts/setup.sh
./scripts/run.sh smoke kafka
./scripts/run.sh smoke kafgres
\`\`\`

Results are written to \`results/<system>/\`.

Kafka and Kafgres are never benchmarked simultaneously on the same host.

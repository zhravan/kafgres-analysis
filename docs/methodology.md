# Benchmark Methodology

Kafka and Kafgres are benchmarked sequentially on the same GitHub Actions runner. Each system is stopped before the other starts, avoiding simultaneous resource contention.

GitHub-hosted runners are shared virtual machines, so CI numbers are intended primarily for relative comparison within the same benchmark run, not as universal hardware benchmarks.

## Test profiles

Smoke:
- 1 producer
- 1 consumer
- 3 partitions
- 1 KiB payload
- 10 second measured interval
- 3 second warm-up

CI/release baseline:
- 11 configurations
- 3 repetitions per configuration
- 20 second measured interval
- 5 second warm-up
- 1 KiB, 10 KiB and 100 KiB payloads
- 1, 3 and 12 partitions for single-producer/single-consumer scaling
- additional 4-producer/4-consumer and 16-producer/16-consumer tests at 1 KiB and 3 partitions

Full matrix is available through `benchmark/matrix.py --profile full` for longer, non-release runs. It covers 100 B, 1 KiB, 10 KiB and 100 KiB; 1, 3, 6 and 12 partitions; and 1, 4 and 16 producer/consumer counts, with 3 repetitions by default.

## Measurement rules

- Both systems use the same Confluent Kafka client and equivalent Kafka protocol configuration.
- Producers use `acks=all`, idempotence, no compression, and the same batching settings.
- Consumer groups use the same client settings and one shared group per benchmark run.
- Payloads contain a send timestamp and are otherwise deterministic filler bytes.
- Producer throughput counts acknowledged records during the measured interval.
- Consumer throughput counts measured records consumed during the interval.
- End-to-end latency is measured from the embedded producer timestamp to consumer receipt.
- Broker resources are sampled from the Docker container with `docker stats`.
- Each configuration is repeated three times; the report uses the median run-level metrics.
- Results are raw measurements; no overall score or winner is generated.

## Publication

Every run records raw JSON measurements, resource samples, benchmark parameters and runner metadata. Release-triggered runs publish both a compressed raw-result bundle and the generated Markdown comparison report as release assets.

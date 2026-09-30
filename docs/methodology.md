# Benchmark Methodology

Kafka and Kafgres are benchmarked sequentially on the same GitHub Actions runner. They are never started simultaneously.

GitHub-hosted runners are shared virtual machines, so CI numbers are intended primarily for relative comparison within the same run, not as universal hardware benchmarks.

## Initial matrix

Smoke: 1 producer, 1 consumer, 3 partitions, 1 KiB messages, 10 second measurement, 3 second warm-up.

Baseline: message sizes 100 B, 1 KiB, 10 KiB, 100 KiB; partitions 1, 3, 6, 12; producers 1, 4, 16; consumers 1, 4, 16; 5 minute runs; minimum 3 repetitions.

## Publication

Every benchmark run records the tested versions, GitHub ref, runner metadata and raw measurements. Release-triggered runs upload the complete raw result bundle as a release asset.

No overall winner or score is generated; results are reported as measurements.

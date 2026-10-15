# Session 1 — Retail-Orders Parallel Compute Pipeline

Adapted from the MIT 261 Session 1 (CMA-Flow) code guide to run against the
12-file retail/order dataset (categories, customers, employees, order_items,
orders, payments, products, promotions, returns, shipments, stores, suppliers).

## What changed from the course template

| Original (CMA-Flow)                      | This version (retail-orders)                              |
|-------------------------------------------|-------------------------------------------------------------|
| 4 files (transactions, customers, entitlements, monetization_configs) | 12 files, 4 on the primary join path (order_items, orders, stores, products) |
| Partition key: `region` (50 values, 1.82:1 skew) | Partition key: `category_id` (30 values, 1.22:1 skew) |
| Rejected: `customer_id` (perfectly uniform 20/20/20) | Rejected: `customer_id` (43:1 skew but per-customer granularity), `store_city` (only 4 values) |
| Metric: `amount` (raw column)             | Metric: `amount` = `qty * unit_price` (derived) |
| All 3 joined parent files broadcast (all tiny) | Only `stores` + `products` broadcast; `orders` (8.5 MB) is a genuine shuffle candidate |
| 1:1 relation found: `entitlements` | 1:1 relations found: `payments` **and** `shipments` (both unique on `order_id`) |
| 11 event rows: 60,000 | 600,000 (`order_items`) |

## Pipeline

Run in this order from inside this folder:

```bash
pip install pandas pyspark pyarrow
python profile_files.py        # Parts 2-3: inventory, 11 FK integrity checks, eligibility
python load_and_join.py        # Part 4: join path + reconciliation
python partition_strategy.py   # Part 5: partition key selection (measured, not asserted)
python sequential_baseline.py  # Part 6: pandas reference result
python parallel_compute.py     # Parts 7, 9: Spark join + aggregation + correctness check
python benchmark.py            # Part 8: parallelism comparison (2/4/8 partitions)
python partition_analysis.py   # Part 10: key-level vs partition-level skew
python render_diagrams.py      # Part 4/11: entity model + architecture diagrams
```

## Sandbox note

This code was developed and tested in an environment **without internet
access**, so PySpark could not be installed here (`pip install pyspark`
fails with no matching distribution). Everything that only needs pandas —
`profile_files.py`, `load_and_join.py`, `partition_strategy.py`,
`sequential_baseline.py`, `render_diagrams.py` — was run end-to-end against
your real CSVs and the numbers below are genuine output, not placeholders.

`parallel_compute.py`, `benchmark.py`, and `partition_analysis.py` are
complete, syntactically checked, and follow the exact same design as the
course template (broadcast hints, `repartition(n, key)`, physical-plan
inspection, `validate()` against the baseline) — but they need PySpark + a
JVM installed to actually run. Install with `pip install pyspark pyarrow`
(requires Java 8/11/17) and run them in your own machine or the course VM.

## Real results already produced (results/)

- `file_profile.json` — full profile of all 12 files, 11/11 FK checks pass, all 4 eligibility conditions met
- `working_dataset.parquet` (or `.pkl` fallback) — the 600,000-row joined frame
- `partition_strategy.json` — every column in the joined frame scored as a partition-key candidate
- `baseline_result.csv` — pandas reference: 30 categories, total revenue 3,827,746,136.00, baseline median 0.0100s over 5 runs

## Diagrams (already rendered)

- `docs/entity-model-session1.png`
- `architecture/architecture-session1.png`

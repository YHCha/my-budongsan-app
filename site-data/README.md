# Site transaction feed

Full-region historical collection is now a separate layer: see
[TRANSACTION_ARCHIVE.md](../TRANSACTION_ARCHIVE.md) for coverage, partition schema,
storage limits and the independent `transaction-archive` branch. This feed remains
the existing Site contract. Additional verified targets enter CSV downloads on the
next successful refresh without changing the 23 UI candidate definitions.

The existing Actions workflow runs daily at 07:17 Asia/Seoul (scheduled starts may be delayed), on changes to the updater, and through workflow_dispatch. No user visit is required.

The existing MOLIT_SERVICE_KEY repository secret stays in Actions. Only Python's standard library is required. The updater re-queries all six districts for each month from January 2025 through yesterday in Korea, verifies pagination and totals, replaces these months, preserves separate identical-looking contracts, and excludes cancellations only from chart summaries. Contracts before 2025 retain the reconciled v26 baseline. KB prices and listing counts retain their original dates and are not refreshed by MOLIT.

latest.json atomically contains the snapshot, CSV, checksum, Korea collection date, query cutoff and Actions run URL. The Site reads that public file without credentials; chart and tables use the same revision. Download links use the same bundle. The prior successful file remains unchanged if any fetch or check fails. Disappearing target months, malformed data, mismatched totals or loss of over 15% of valid records abort publication.

seed-market.json and seed-transactions.csv are the public v26 snapshot from 2026-10-21. They contain no personal budget inputs or keys. Do not edit their provenance dates to make old data appear current.

main.py and regional CSVs remain legacy Streamlit collector inputs. The workflow now publishes the Site feed; it does not run the old append/deduplicate path or update the Streamlit datasets.

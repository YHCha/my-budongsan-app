# Site transaction feed

Full-region historical collection is now a separate layer: see
[TRANSACTION_ARCHIVE.md](../TRANSACTION_ARCHIVE.md) for coverage, partition schema,
storage limits and the independent `transaction-archive` branch. This feed remains
the existing Site contract. Additional verified targets enter CSV downloads on the
next successful refresh without changing the 23 UI candidate definitions.

## Expanded candidate and move-up analysis

`candidate-catalog.json` retains the original 23 transaction complexes and adds 15
verified complexes, their observed exact floor areas, independent complex IDs,
core/watch/archive statuses, explicit pooled groups and dated public sources.
Unknown property facts and unverified portal IDs stay null. It contains no private
financial context. Central Xi has observed 72-square-metre types, not a fabricated
59 type. Bando 7/8 retain their observed 73 and 76.1872 types.

The daily updater reads this catalog to expand tracked names/area ranges across
the same seven districts. The existing schedule, secret, pagination and full-month
replacement guards remain. `latest.json` retains schemaVersion 1 and the original
23 metadata definitions, adding `trackedTargets` for validation of extra CSV rows.

`build_analysis_feed.py` verifies the archive manifest/checksums, replaces archive
months with current daily refreshes where the target and partition are explicitly
covered, and builds `analysis.json`. Pre-2020 baseline rows retain their original
provenance. All matching archive contracts from 2020 are preserved, including
cancellations and identical-looking separate contracts. The Site filters cancelled
rows for charts and summaries and computes matched calendar-window medians/rates.
Both feeds are committed together only after collection and analysis validation
succeed. An archive checkout/build failure prevents publication of either new feed.

The analysis feed's collection date is the daily feed date; historical partition
freshness is separately recorded as `archive.checkedAtMin/Max`. That date does not
claim that all historical months were fetched again that day. Source branch and
archive commit remain explicit. No Site redeployment is needed for later successful
data refreshes; catalog/schema changes still require compatible Site code.

The existing Actions workflow runs daily at 07:17 Asia/Seoul (scheduled starts may be delayed), on changes to the updater, and through workflow_dispatch. No user visit is required.

The existing MOLIT_SERVICE_KEY repository secret stays in Actions. Only Python's standard library is required. The updater re-queries all six districts for each month from January 2025 through yesterday in Korea, verifies pagination and totals, replaces these months, preserves separate identical-looking contracts, and excludes cancellations only from chart summaries. Contracts before 2025 retain the reconciled v26 baseline. KB prices and listing counts retain their original dates and are not refreshed by MOLIT.

latest.json atomically contains the snapshot, CSV, checksum, Korea collection date, query cutoff and Actions run URL. The Site reads that public file without credentials; chart and tables use the same revision. Download links use the same bundle. The prior successful file remains unchanged if any fetch or check fails. Disappearing target months, malformed data, mismatched totals or loss of over 15% of valid records abort publication.

seed-market.json and seed-transactions.csv are the public v26 snapshot from 2026-10-21. They contain no personal budget inputs or keys. Do not edit their provenance dates to make old data appear current.

main.py and regional CSVs remain legacy Streamlit collector inputs. The workflow now publishes the Site feed; it does not run the old append/deduplicate path or update the Streamlit datasets.

Geumbit Grand Maison complex 1 (41133, 금광동 1001, exact API name
e편한세상금빛그랑메종1단지) is now an extended watch candidate. Verified focus
areas: 59.95/59.98 square metres; 74 and 84 profiles remain selectable. The catalog
now tracks 38 transaction complexes; no 51-square-metre type is fabricated for complex 1.

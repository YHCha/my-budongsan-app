# Two-layer transaction collection

## Compatibility and publication

`site-data/latest.json` remains schemaVersion 1: snapshot metadata, CSV, CSV SHA-256,
and generated time in one atomic file. Existing daily workflow, URL, 23 UI candidate
definitions, ordering, price assumptions and legacy Streamlit files are retained.
The Site source/deployment is not in this repository; compatibility is verified
against the existing bundle contract, not a live browser deployment.

An independent daily workflow (07:47 Asia/Seoul, scheduling can be delayed) publishes
`archive/manifest.json` and `archive/<region>/<YYYYMM>.json.gz` on the dedicated
`transaction-archive` branch. It does not deploy a Site or commit archive files to
main. An archive failure does not stop the existing Site collection workflow.
Both use only the existing Actions secret, never client-side credentials.

## Coverage and reconciliation

Six exact existing Site codes: 11170, 11680, 11710, 41131, 41135, 41597.
The legacy main.py has only five codes and is deliberately not used. The archive
starts January 2020. Codes are held fixed for compatibility: administrative boundary
or code changes need a separately verified migration, not guessed remapping.

Every run fetches all six latest contract months, up to 12 missing region-months
(oldest first), and up to 12 oldest checked historical partitions. A new archive
starts with 48 region-month requests, normally at most 60 after auditing begins;
pagination can increase API calls. Initial historical coverage takes about 39 daily
runs at the October 2026 horizon. Missing partitions remain explicitly missing,
not zero transactions. Check completedPartitions/expectedPartitions and
backfillComplete before claiming full historical coverage. Historical audit latency
is roughly (historical partitions / 12) successful runs, not immediate. checkedAt
provides exact freshness per partition. New months keep entering the plan.

Each successful complete API response replaces the entire region-month multiset.
Changed prices, disappeared contracts and cancellation fields replace stale states;
no append/identity merge and no deduplication by apparent contract attributes.
Separate identical-looking contracts are preserved. Cancelled rows remain in the
archive; analysts must exclude cdealType/cdealDay/cancelDealDay as appropriate.
All returned fields in the documented public-field allowlist are preserved, including
aptSeq, building year and registration date; unknown API fields are intentionally
discarded. Schema extensions require code review. Current-month archive rows may
include today's contracts; the Site retains its existing yesterday cutoff.

Reuses the existing bounded pagination, repeated-page detection, totals checks,
timeouts and three network/XML attempts. Every record is validated for complex
name, region (when supplied), contract date/month, finite valid area, positive
price and floor parsing. Checksums and partition counts are checked on load and
after writes. A loss exceeding 25% of a previously archived partition aborts the
run; investigate API completeness before changing that guard. Successful zero
results for previously empty/missing months are legitimate. Cancellation status
changes do not reduce raw row counts and remain publishable.

## Storage and failure safety

Deterministic gzip, sorted records, no collection timestamp inside each partition:
unchanged API data produces identical bytes. Manifest holds SHA-256, compressed
bytes, row count and last successful check per partition. Limits: 8 MiB per file,
200 MiB compressed total. Exceeding a limit fails without deleting old data;
migrate to object storage before expanding the horizon or geographic scope.

The dedicated branch is intentionally a single orphan snapshot commit, published
with an exact force-with-lease against its previously inspected head. Only this
dedicated data branch is replaced; main is never force pushed. This bounds
reachable archive history rather than adding every daily snapshot to main's
permanent Git history. GitHub may retain unreachable objects/reflogs temporarily;
200 MiB is not a promise about server physical storage or clones retained by others.
Do not tag/archive every snapshot in this repository. Clone that branch with depth 1.
If branch rules prohibit replacement, the push fails safely: configure an appropriate
dedicated data destination rather than disabling protections on main.

All fetches and checks complete before local writes begin. Local filesystem failures
can leave an unpublished work directory partially updated, so discard/reload that
work directory on failure. Remote readers see only a successful atomic Git ref
update. Collection, checksum, disk, size, lease or permission failures retain the
previous remote snapshot. Remote inspection errors must not be mistaken for a
missing archive branch. No URLs containing a key or raw exception bodies are logged.

## Additional tracked targets

On the next successful Site API refresh, the CSV additionally includes exact verified
names for Bando 5.0 (59/74), Sanseong Xi Prugio 1/3 (51/59), and 2 (59).
Bando 6.0 already retains all sizes, including 59/74. The old UI candidate cards
are intentionally not expanded without the external Site consumer's schema.
Extra CSV rows affect aggregate row counts and downloads but not existing candidate
definitions. Their pre-2025 history is not backfilled into the Site seed; use the
archive after those partitions are fetched. Do not import the legacy deduplicated
CSVs as authoritative history. Sanseong 2's 51 type was not observed in those CSVs.
Sinheung Haneulchae is deferred: the legacy CSV name is 코오롱하늘채; an exact
identity/address match is required before adding an alias to a public feed.

## Operation and verification

```
python -m unittest discover -s tests -v
python scripts/archive_transactions.py --plan
python scripts/archive_transactions.py --archive-dir /path/to/previous/archive --dry-run
```

`--plan` needs no key or network and writes nothing. `--dry-run` needs the Actions
secret in the environment, fetches/validates the planned batch, and writes nothing.
Never paste keys into command arguments, issue bodies or repository files.
For initial population, run the new workflow manually or wait for its scheduled run;
inspect its success and the branch manifest. Repeat successful runs to backfill.
Do not reset the manifest to accelerate backfill. Never claim the archive is fully
populated merely because the pipeline has been installed.

Offline tests cover pagination (existing suite), cancellation replacement,
identical contracts, untracked complexes, invalid records, rolling/audit planning,
no-write dry runs, late-fetch failure, checksum corruption and size/drop guards.
No live API validation is possible locally without the secret; first Actions run
is the integration check. The private PROJECT_CONTEXT.md and financial assumptions
are not inputs to this pipeline. The code publication allowlist contains only
collector source, tests, workflows, this design and ignore rules.

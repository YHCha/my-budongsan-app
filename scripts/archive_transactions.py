"""Full-region MOLIT snapshots, independent of the deployed Site feed (stdlib only)."""
import argparse
from datetime import date, datetime, timedelta
import gzip
import hashlib
import json
from pathlib import Path
import os
import subprocess
import time

from update_site_transactions import CollectionError, KST, fetch_month, fetch_page, months, normalize, norm

LEGACY_REGIONS = ('11170', '11680', '11710', '41131', '41135', '41597')
REGIONS = ('11170', '11680', '11710', '41131', '41133', '41135', '41597')
START = '2020-01'
MAX_PARTITION = 8 * 1024 * 1024
MAX_ARCHIVE = 200 * 1024 * 1024
MIN_REQUEST_INTERVAL = 1.0
MAX_PAGE_CALLS = 2000  # At most 6000 HTTP attempts with the shared three-attempt retry.
# Public API fields only; never persist response headers, request URLs or environment.
FIELDS = frozenset('aptNm aptSeq aptDong buildYear dealYear dealMonth dealDay dealAmount excluUseAr floor umdNm jibun sggCd rgstDate dealingGbn reqGbn cancelDealDay cdealDay cdealType buyerGbn slerGbn estateAgentSggNm landLeaseholdGbn'.split())


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def plan(manifest, cutoff, batch=12):
    """Recent 6 months + bounded oldest-missing backfill + oldest checked audit."""
    entries = manifest.get('partitions', {})
    all_jobs = [(code, month) for month in months(START, cutoff) for code in REGIONS]
    recent_start = date(cutoff.year + (cutoff.month - 6) // 12, (cutoff.month - 6) % 12 + 1, 1)
    recent = {job for job in all_jobs if job[1] >= recent_start.strftime('%Y%m')}
    missing = [j for j in all_jobs if '/'.join(j) not in entries and j not in recent][:batch]
    audit = sorted((j for j in all_jobs if '/'.join(j) in entries and j not in recent),
                   key=lambda j: (entries['/'.join(j)]['checkedAt'], j))[:batch]
    return sorted(recent | set(missing) | set(audit))


def missing_jobs(manifest, cutoff):
    """Fixed horizon, oldest month first; an indexed zero-row partition is complete."""
    entries = manifest.get('partitions', {})
    return [(code, month) for month in months(START, cutoff) for code in REGIONS
            if code + '/' + month not in entries]


class PacedPages:
    """Sequential page calls, bounded per execution; shared retries remain unchanged."""
    def __init__(self, fetcher=fetch_page, clock=time.monotonic, sleep=time.sleep):
        self.fetcher, self.clock, self.sleep = fetcher, clock, sleep
        self.last, self.calls = None, 0

    def __call__(self, *args):
        if self.calls >= MAX_PAGE_CALLS:
            raise CollectionError('Archive page request budget exhausted; resume from checkpoint')
        if self.last is not None:
            self.sleep(max(0, MIN_REQUEST_INTERVAL - (self.clock() - self.last)))
        self.last = self.clock()
        self.calls += 1
        return self.fetcher(*args)


def partition(code, month, items, checked):
    records = []
    for item in items:
        name = item.get('aptNm', '').strip()
        if not name or item.get('sggCd', code) != code:
            raise CollectionError(f'{code} {month}: invalid name/region')
        # Validate EVERY record, including cancelled and untracked complexes.
        normalize(item, code, month, {(code, norm(name)): name}, checked[:10])
        records.append({k: v for k, v in item.items() if k in FIELDS})
    records.sort(key=lambda r: encoded(r))  # Stable multiset; do not deduplicate contracts.
    payload = {'schemaVersion': 1, 'region': code, 'month': month, 'records': records}
    data = gzip.compress(encoded(payload), mtime=0)
    if len(data) > MAX_PARTITION:
        raise CollectionError('Partition size limit exceeded')
    return data


def load(directory):
    path = directory / 'manifest.json'
    if not path.exists():
        if any(directory.rglob('*.json.gz')):
            raise CollectionError('Archive manifest missing')
        return {'schemaVersion': 1, 'startMonth': START, 'regions': list(REGIONS), 'partitions': {}}
    manifest = json.loads(path.read_text(encoding='utf-8'))
    if (manifest.get('schemaVersion') != 1
            or manifest.get('startMonth') != START
            or manifest.get('regions') not in (list(REGIONS), list(LEGACY_REGIONS))):
        raise CollectionError('Unsupported archive manifest')
    # Explicit additive migration: keep all six existing regions and their checksums.
    # New district partitions enter the normal bounded missing-month backfill.
    manifest['regions'] = list(REGIONS)
    expected = set()
    for key, entry in manifest['partitions'].items():
        code, month = key.split('/')
        if code not in REGIONS or len(month) != 6 or not month.isdigit():
            raise CollectionError('Invalid partition key')
        date(int(month[:4]), int(month[4:]), 1)
        path = directory / code / (month + '.json.gz')
        expected.add(path)
        data = path.read_bytes()
        if len(data) > MAX_PARTITION:
            raise CollectionError('Partition size limit exceeded')
        if len(data) != entry['bytes'] or hashlib.sha256(data).hexdigest() != entry['sha256']:
            raise CollectionError('Archive checksum mismatch')
        payload = json.loads(gzip.decompress(data))
        if payload['region'] != code or payload['month'] != month or len(payload['records']) != entry['rows']:
            raise CollectionError('Archive metadata mismatch')
    if set(directory.rglob('*.json.gz')) != expected:
        raise CollectionError('Unindexed archive partition')
    if sum(e['bytes'] for e in manifest['partitions'].values()) > MAX_ARCHIVE:
        raise CollectionError('Archive size budget exceeded')
    return manifest


def collect(directory, cutoff, fetcher, dry_run=False, batch=12, now=None, jobs=None):
    manifest = load(directory)
    checked = now or datetime.now(KST).isoformat()
    pending = {}
    selected = plan(manifest, cutoff, batch) if jobs is None else jobs
    for code, month in selected:
        key = code + '/' + month
        items = fetcher(code, month)
        data = partition(code, month, items, checked)
        previous = manifest['partitions'].get(key)
        # A successful but suspicious drop requires manual investigation, never silent deletion.
        if previous and previous['rows'] and len(items) < previous['rows'] * .75:
            raise CollectionError(f'{key}: over 25% of records disappeared')
        pending[key] = data
        manifest['partitions'][key] = {'rows': len(items), 'bytes': len(data),
            'sha256': hashlib.sha256(data).hexdigest(), 'checkedAt': checked}
        if jobs is not None:
            print(f'Validated {key}: {len(items)} records', flush=True)
    if sum(e['bytes'] for e in manifest['partitions'].values()) > MAX_ARCHIVE:
        raise CollectionError('Archive size budget exceeded; move to object storage before expanding')
    total = len(REGIONS) * len(list(months(START, cutoff)))
    manifest.update(contractMonthThrough=cutoff.strftime('%Y%m'),
                    completedPartitions=len(manifest['partitions']), expectedPartitions=total,
                    backfillComplete=len(manifest['partitions']) == total,
                    collectorCodeCommit=os.environ.get('GITHUB_SHA', ''))
    if dry_run:
        return manifest
    directory.mkdir(parents=True, exist_ok=True)
    # All API calls and validation finish before any write. Workflow publishes only on success.
    for key, data in pending.items():
        path = directory / (key + '.json.gz')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.with_suffix('.tmp').write_bytes(data)
        path.with_suffix('.tmp').replace(path)
    temp = directory / 'manifest.tmp'
    temp.write_bytes(encoded(manifest) + b'\n')
    temp.replace(directory / 'manifest.json')
    load(directory)
    return manifest


def full_backfill(directory, cutoff, fetcher, batch_size=75, checkpoint=None, dry_run=False):
    """Validate/write/publish each missing-only batch before starting the next.

    A failed batch is never published. A failed checkpoint stops collection. The
    next runner must load the last remote snapshot, skipping its verified entries.
    Dry runs validate only the next batch and never write or invoke checkpoint.
    """
    if batch_size not in range(50, 101):
        raise CollectionError('Full-backfill batch size must be between 50 and 100')
    result = load(directory)
    batch_number = 0
    while True:
        jobs = missing_jobs(result, cutoff)[:batch_size]
        if not jobs:
            return result
        batch_number += 1
        print(f'Backfill batch {batch_number}: {len(jobs)} missing partitions', flush=True)
        result = collect(directory, cutoff, fetcher, jobs=jobs, dry_run=dry_run)
        if dry_run:
            return result
        if checkpoint is not None:
            checkpoint()
        print(f"Checkpoint {batch_number}: {result['completedPartitions']}/"
              f"{result['expectedPartitions']} partitions validated", flush=True)


def publish_checkpoint(directory, expected):
    """Single orphan snapshot with an exact lease; never touch main or print git errors."""
    directory = directory.resolve()
    if directory.name != 'archive':
        raise CollectionError('Checkpoint publication requires an archive worktree')
    load(directory)
    def git(*args):
        try:
            return subprocess.run(['git', '-C', str(directory.parent), *args],
                                  check=True, capture_output=True, text=True, timeout=120).stdout.strip()
        except (subprocess.SubprocessError, OSError):
            raise CollectionError('Checkpoint publication failed; remote snapshot retained') from None
    if Path(git('rev-parse', '--show-toplevel')).resolve() != directory.parent:
        raise CollectionError('Archive must be at the worktree root')
    # Do not check out/reset the worktree: it contains the newly validated files.
    git('read-tree', '--empty')
    git('add', '-f', '--', 'archive')
    tree = git('write-tree')
    commit = git('commit-tree', tree, '-m', 'Publish validated full-backfill checkpoint')
    git('push', '--force-with-lease=refs/heads/transaction-archive:' + expected,
        'origin', commit + ':refs/heads/transaction-archive')
    return commit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive-dir', type=Path, default=Path('archive'))
    parser.add_argument('--plan', action='store_true', help='No network, key or writes')
    parser.add_argument('--dry-run', action='store_true', help='Fetch and validate, write nothing')
    parser.add_argument('--full-backfill', action='store_true', help='Finish all missing partitions at a fixed horizon')
    parser.add_argument('--batch-size', type=int, default=75, help='Full-backfill checkpoint batch, 50 to 100 (default 75)')
    parser.add_argument('--publish-checkpoints', action='store_true', help='Publish each full-backfill batch with ARCHIVE_EXPECTED lease')
    args = parser.parse_args()
    cutoff = datetime.now(KST).date() - timedelta(days=1)
    if args.full_backfill and not 50 <= args.batch_size <= 100:
        parser.error('--batch-size must be between 50 and 100')
    if args.publish_checkpoints and (not args.full_backfill or args.dry_run or args.plan):
        parser.error('--publish-checkpoints requires a non-dry-run full backfill')
    if args.plan:
        manifest = load(args.archive_dir)
        jobs = missing_jobs(manifest, cutoff) if args.full_backfill else plan(manifest, cutoff)
        print(json.dumps({'jobs': jobs, 'requestsAtLeast': len(jobs),
                          'completedPartitions': len(manifest['partitions']),
                          'expectedPartitions': len(REGIONS) * len(list(months(START, cutoff))),
                          'batchSize': args.batch_size if args.full_backfill else 12}))
        return
    key = os.environ.get('MOLIT_SERVICE_KEY', '').strip()
    if not key:
        raise CollectionError('MOLIT_SERVICE_KEY is not configured')
    pages = PacedPages()
    fetcher = lambda c, m: fetch_month(key, c, m, fetcher=pages)
    if args.full_backfill:
        expected = os.environ.get('ARCHIVE_EXPECTED')
        if args.publish_checkpoints and expected is None:
            raise CollectionError('ARCHIVE_EXPECTED lease is required')
        def checkpoint():
            nonlocal expected
            expected = publish_checkpoint(args.archive_dir, expected)
        result = full_backfill(args.archive_dir, cutoff, fetcher, args.batch_size,
                               checkpoint if args.publish_checkpoints else None, args.dry_run)
    else:
        result = collect(args.archive_dir, cutoff, fetcher, args.dry_run)
    print(f"Validated {result['completedPartitions']}/{result['expectedPartitions']} partitions")


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # Do not print exception bodies: network errors may contain credential-bearing URLs.
        print('Archive collection failed; previous remote snapshot retained. Run offline tests and inspect partition counts.')
        raise SystemExit(1) from None

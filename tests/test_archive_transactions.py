import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import archive_transactions as a
from datetime import date
import gzip
import json
import tempfile
import subprocess
import unittest
from unittest.mock import patch


class ArchiveTests(unittest.TestCase):
    def item(self, **updates):
        item = dict(aptNm='Untracked complex', dealYear='2020', dealMonth='1', dealDay='2',
                    excluUseAr='59.9', floor='1', dealAmount='10,000', cdealType='')
        item.update(updates)
        return item

    def test_full_region_multiset_cancellation_and_allowlist(self):
        row = self.item(cdealType='O', serviceKey='do-not-store')
        data = a.partition('11170', '202001', [row, row], '2026-10-31')
        result = json.loads(gzip.decompress(data))
        self.assertEqual(len(result['records']), 2)
        self.assertEqual(result['records'][0]['cdealType'], 'O')
        self.assertNotIn(b'do-not-store', gzip.decompress(data))
        self.assertEqual(data, a.partition('11170', '202001', [row, row], '2027-01-01'))

    def test_invalid_untracked_records_fail(self):
        for changes in [dict(dealDay='32'), dict(excluUseAr='NaN'), dict(sggCd='11680'), dict(aptNm='')]:
            with self.assertRaises(a.CollectionError):
                a.partition('11170', '202001', [self.item(**changes)], '2026-10-31')

    def test_planner_backfill_recent_and_oldest_audit(self):
        jobs = a.plan({}, date(2026, 10, 30))
        self.assertEqual(len(jobs), 54)
        self.assertIn(('41133', '202610'), jobs)
        self.assertIn(('11170', '202001'), jobs)
        self.assertIn(('41597', '202610'), jobs)
        entries = {'11170/202001': {'checkedAt': '2020-01-01'}}
        self.assertIn(('11170', '202001'), a.plan({'partitions': entries}, date(2026, 10, 30)))

    def test_replacement_removes_stale_cancellation_and_preserves_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(a, 'plan', return_value=[('11170', '202001')]):
            directory = Path(tmp)
            a.collect(directory, date(2026, 10, 30), lambda *_: [self.item(), self.item()], now='2026-10-30')
            a.collect(directory, date(2026, 10, 30), lambda *_: [self.item(cdealType='O'), self.item()], now='2026-10-31')
            rows = json.loads(gzip.decompress((directory/'11170/202001.json.gz').read_bytes()))['records']
            self.assertEqual(len(rows), 2)
            self.assertEqual(sum(r['cdealType'] == 'O' for r in rows), 1)

    def test_failure_dry_run_corruption_and_size_safety(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(a, 'plan', return_value=[('11170', '202001')]):
            directory = Path(tmp)
            a.collect(directory, date(2026, 10, 30), lambda *_: [self.item()], dry_run=True)
            self.assertEqual(list(directory.iterdir()), [])
            a.collect(directory, date(2026, 10, 30), lambda *_: [self.item()])
            before = {str(p): p.read_bytes() for p in directory.rglob('*') if p.is_file()}
            for fetcher in [lambda *_: [], lambda *_: [self.item(dealDay='32')]]:
                with self.assertRaises(a.CollectionError):
                    a.collect(directory, date(2026, 10, 30), fetcher)
            with patch.object(a, 'MAX_ARCHIVE', 1), self.assertRaises(a.CollectionError):
                a.collect(directory, date(2026, 10, 30), lambda *_: [self.item()])
            self.assertEqual(before, {str(p): p.read_bytes() for p in directory.rglob('*') if p.is_file()})
            (directory/'11170/202001.json.gz').write_bytes(b'corrupt')
            with self.assertRaises(a.CollectionError):
                a.load(directory)

    def test_later_partition_failure_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(a, 'plan', return_value=[('11170', '202001'), ('11680', '202001')]):
            def fetch(code, month):
                if code == '11680':
                    raise a.CollectionError('unavailable')
                return [self.item()]
            with self.assertRaises(a.CollectionError):
                a.collect(Path(tmp), date(2026, 10, 30), fetch)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_six_region_manifest_migrates_without_losing_partitions(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(a, 'plan', return_value=[('11170', '202001')]):
            directory = Path(tmp)
            a.collect(directory, date(2026, 10, 30), lambda *_: [self.item()])
            path = directory/'manifest.json'
            manifest = json.loads(path.read_text())
            entries = manifest['partitions'].copy()
            manifest['regions'] = list(a.LEGACY_REGIONS)
            path.write_bytes(a.encoded(manifest))
            migrated = a.load(directory)
            self.assertEqual(migrated['regions'], list(a.REGIONS))
            self.assertEqual(migrated['partitions'], entries)

    def test_landers_targets_are_in_jungwon_not_sujeong(self):
        from update_site_transactions import EXTRA_TARGETS
        for number in (1, 2, 3):
            name = f'신흥역하늘채랜더스원{number}단지'
            self.assertIn((59, 60), EXTRA_TARGETS[('41133', name)])
            self.assertNotIn(('41131', name), EXTRA_TARGETS)

    def test_repeated_orphan_publication_preserves_changed_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=root, stderr=subprocess.STDOUT).decode().strip()
            git('init')
            git('config', 'user.name', 'Archive Test')
            git('config', 'user.email', 'archive-test@example.invalid')
            (root/'archive').mkdir()
            (root/'archive/manifest.json').write_text('first')
            (root/'source.txt').write_text('not in archive snapshot')
            git('add', '.')
            git('commit', '-m', 'initial')
            for i in (1, 2):
                (root/'archive/manifest.json').write_text(str(i))
                git('checkout', '--orphan', f'snapshot-{i}')
                git('read-tree', '--empty')
                git('add', '--', 'archive')
                git('commit', '-m', 'snapshot')
                self.assertEqual(git('ls-tree', '-r', '--name-only', 'HEAD'), 'archive/manifest.json')
                self.assertEqual(git('show', 'HEAD:archive/manifest.json'), str(i))
                self.assertEqual(git('rev-list', '--count', 'HEAD'), '1')

    def historic_item(self, code, month):
        return self.item(sggCd=code, dealYear=month[:4], dealMonth=str(int(month[4:])))

    def test_full_backfill_496_missing_in_sequential_75_partition_checkpoints(self):
        cutoff = date(2026, 10, 30)
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            all_jobs = a.missing_jobs({}, cutoff)
            fetch = lambda c, m: [self.historic_item(c, m)]
            a.collect(directory, cutoff, fetch, jobs=all_jobs[:78])
            old = {key: entry.copy() for key, entry in a.load(directory)['partitions'].items()}
            calls, checkpoints = [], []
            def collect(code, month):
                calls.append((code, month))
                row = self.historic_item(code, month)
                return [row, row, dict(row, cdealType='O')]
            def checkpoint():
                checkpoints.append(a.load(directory)['completedPartitions'])
            result = a.full_backfill(directory, cutoff, collect, checkpoint=checkpoint)
            self.assertEqual(checkpoints, [153, 228, 303, 378, 453, 528, 574])
            self.assertEqual(calls, all_jobs[78:])
            self.assertEqual(len(calls), 496)
            self.assertTrue(result['backfillComplete'])
            for key, entry in old.items():
                self.assertEqual(result['partitions'][key], entry)
            payload = json.loads(gzip.decompress((directory / ('/'.join(calls[0]) + '.json.gz')).read_bytes()))
            self.assertEqual(len(payload['records']), 3)
            self.assertEqual(sum(r['cdealType'] == 'O' for r in payload['records']), 1)
            def forbidden(*args):
                self.fail('Completed backfill must not fetch or publish again')
            a.full_backfill(directory, cutoff, forbidden, checkpoint=forbidden)

    def test_full_backfill_resumes_from_last_published_batch_after_fetch_failure(self):
        cutoff = date(2020, 10, 30)  # 70 partitions, two batches at size 50.
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            calls, published = [], []
            def fetch(code, month):
                calls.append((code, month))
                if len(calls) == 52:
                    raise a.CollectionError('simulated API failure')
                return [self.historic_item(code, month)]
            def checkpoint():
                published.append(a.load(directory)['completedPartitions'])
            with self.assertRaises(a.CollectionError):
                a.full_backfill(directory, cutoff, fetch, 50, checkpoint)
            self.assertEqual(published, [50])
            self.assertEqual(a.load(directory)['completedPartitions'], 50)
            resumed = []
            def resume(code, month):
                resumed.append((code, month))
                return [self.historic_item(code, month)]
            result = a.full_backfill(directory, cutoff, resume, 50, checkpoint)
            self.assertEqual(resumed, a.missing_jobs({}, cutoff)[50:])
            self.assertEqual(published, [50, 70])
            self.assertTrue(result['backfillComplete'])

    def test_failed_checkpoint_stops_before_any_next_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls = []
            def fetch(code, month):
                calls.append((code, month))
                return [self.historic_item(code, month)]
            def unavailable():
                raise a.CollectionError('lease rejected')
            with self.assertRaises(a.CollectionError):
                a.full_backfill(Path(tmp), date(2020, 10, 30), fetch, 50, unavailable)
            self.assertEqual(len(calls), 50)

    def test_full_backfill_plan_zero_rows_dry_run_and_batch_bounds(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            cutoff = date(2020, 10, 30)
            result = a.full_backfill(directory, cutoff, lambda *_: [], 50, dry_run=True,
                                     checkpoint=lambda: self.fail('Dry run published'))
            self.assertEqual(result['completedPartitions'], 50)
            self.assertEqual(list(directory.iterdir()), [])
            a.full_backfill(directory, cutoff, lambda *_: [], 100)
            self.assertEqual(a.missing_jobs(a.load(directory), cutoff), [])
            for size in (0, 12, 49, 101):
                with self.assertRaises(a.CollectionError):
                    a.full_backfill(directory, cutoff, lambda *_: [], size)

    def test_pacing_and_per_execution_request_budget(self):
        clock, pauses, calls = [0.0], [], []
        def sleep(seconds):
            pauses.append(seconds)
            clock[0] += seconds
        def fetch(*args):
            calls.append(args)
            return 0, []
        pages = a.PacedPages(fetch, lambda: clock[0], sleep)
        with patch.object(a, 'MAX_PAGE_CALLS', 2):
            pages('not-a-key', '11170', '202001', 1)
            pages('not-a-key', '11170', '202002', 1)
            with self.assertRaises(a.CollectionError):
                pages('not-a-key', '11170', '202003', 1)
        self.assertEqual(pauses, [a.MIN_REQUEST_INTERVAL])
        self.assertEqual(len(calls), 2)

    def test_real_checkpoint_git_publication_and_concurrent_writer_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            remote, checkout = root/'remote.git', root/'checkout'
            def git(*args, cwd=root):
                return subprocess.check_output(['git', *args], cwd=cwd, stderr=subprocess.STDOUT).decode().strip()
            git('init', '--bare', str(remote))
            git('init', str(checkout))
            git('config', 'user.name', 'Archive Test', cwd=checkout)
            git('config', 'user.email', 'archive-test@example.invalid', cwd=checkout)
            git('remote', 'add', 'origin', str(remote), cwd=checkout)
            (checkout/'source.txt').write_text('must not be published')
            git('add', '.', cwd=checkout)
            git('commit', '-m', 'source', cwd=checkout)
            directory = checkout/'archive'
            a.collect(directory, date(2020, 1, 30), lambda *_: [], jobs=[('11170', '202001')])
            first = a.publish_checkpoint(directory, '')
            a.collect(directory, date(2020, 1, 30), lambda *_: [], jobs=[('11680', '202001')])
            second = a.publish_checkpoint(directory, first)
            self.assertNotEqual(first, second)
            self.assertEqual(git('rev-parse', 'refs/heads/transaction-archive', cwd=remote), second)
            files = git('ls-tree', '-r', '--name-only', second, cwd=remote).splitlines()
            self.assertTrue(all(p.startswith('archive/') for p in files))
            self.assertEqual(git('rev-list', '--count', second, cwd=remote), '1')
            # An old lease cannot overwrite a newer remote checkpoint.
            a.collect(directory, date(2020, 1, 30), lambda *_: [], jobs=[('11710', '202001')])
            with self.assertRaises(a.CollectionError):
                a.publish_checkpoint(directory, first)
            self.assertEqual(git('rev-parse', 'refs/heads/transaction-archive', cwd=remote), second)

    def test_load_rejects_existing_oversize_archive_before_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            a.collect(directory, date(2020, 1, 30), lambda *_: [], jobs=[('11170', '202001')])
            for limit in ('MAX_PARTITION', 'MAX_ARCHIVE'):
                with patch.object(a, limit, 1), self.assertRaises(a.CollectionError):
                    a.load(directory)

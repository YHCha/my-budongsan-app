import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import archive_transactions as a
from datetime import date
import gzip
import json
import tempfile
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
        self.assertEqual(len(jobs), 48)
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

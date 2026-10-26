import importlib.util
import json
from pathlib import Path
import unittest
from datetime import date

spec = importlib.util.spec_from_file_location('updater', Path(__file__).resolve().parents[1]/'scripts/update_site_transactions.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class RefreshTests(unittest.TestCase):
    def row(self, **overrides):
        row = dict.fromkeys(m.COLS, '')
        row.update({'지역코드':'41135','단지명':'산운마을11단지(판교포레라움)','거래일':'2026-09-01','전용면적':'51.16','층':'1','거래금액(만원)':'142000'})
        row.update(overrides)
        return row

    def test_replacement_preserves_separate_identical_contracts(self):
        old = [self.row(), self.row(**{'거래일':'2020-01-01'})]
        fresh = [self.row(**{'거래금액(만원)':'141000'}),self.row(**{'거래금액(만원)':'141000'})]
        result = m.replace_months(old,fresh,{('41135','202609')})
        self.assertEqual(len(result),3)
        self.assertEqual([r['거래금액(만원)'] for r in result].count('141000'),2)

    def test_cancellation_variants(self):
        for field,value in [('해제사유발생일','26.10.01'),('기타_cdealDay','20261001'),('해제여부','Y')]:
            self.assertTrue(m.cancelled(self.row(**{field:value})))
        self.assertFalse(m.cancelled(self.row()))

    def test_pagination_and_failure(self):
        calls = []
        def page(key,code,month,n):
            calls.append(n)
            return 3, [{'n':1},{'n':2}] if n==1 else [{'n':3}]
        self.assertEqual(len(m.fetch_month('not-a-key','41135','202609',page)),3)
        self.assertEqual(calls,[1,2])
        with self.assertRaises(m.CollectionError):
            m.fetch_month('not-a-key','41135','202609',lambda *a:(3,[]))
        with self.assertRaises(m.CollectionError):
            m.fetch_month('not-a-key','41135','202609',lambda *a:(1,[{},{}]))

    def test_empty_replacement_aborts(self):
        with self.assertRaises(m.CollectionError):
            m.replace_months([self.row()],[],{('41135','202609')})

    def test_normalization_and_invalid_date(self):
        target = {('41135',m.norm('산운마을11단지(판교포레라움)')):'산운마을11단지(판교포레라움)'}
        item = {'aptNm':'산운마을11단지판교포레라움','dealYear':'2026','dealMonth':'9','dealDay':'1','excluUseAr':'51.16','floor':'1','dealAmount':'142,000','cdealType':'O','dealingGbn':'직거래'}
        row = m.normalize(item,'41135','202609',target,'2026-10-26')
        self.assertTrue(m.cancelled(row))
        self.assertEqual(row['거래유형'],'직거래')
        with self.assertRaises(m.CollectionError):
            m.normalize(item,'41135','202610',target,'2026-10-26')

    def test_seed_summary_all_complexes_and_separate_sizes(self):
        import csv, gzip, io
        seed = json.loads(gzip.decompress((m.ROOT/'site-data/seed-market.json.gz').read_bytes()).decode('utf-8'))
        text = gzip.decompress((m.ROOT/'site-data/seed-transactions.csv.gz').read_bytes()).decode('utf-8-sig')
        rows = list(csv.DictReader(io.StringIO(text)))
        result = m.summarize(seed,rows,'2026-10-26',date(2026,10,25),[])
        self.assertEqual(result['includedRows'],7655)
        self.assertEqual(len(result['complexes']),23)
        self.assertEqual(result['kbAsOf'],'2026-10-21')
        self.assertEqual([result['complexes'][i]['lo'] for i in [8,9,13]],[51,50,49])
        self.assertEqual(seed['asOf'],'2026-10-21')
        self.assertEqual(len(m.bundle_data(result,rows)['csvSha256']),64)

if __name__ == '__main__':
    unittest.main()

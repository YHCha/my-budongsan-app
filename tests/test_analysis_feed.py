import csv
import gzip
import hashlib
import io
import json
from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import archive_transactions as archive
import build_analysis_feed as analysis
import update_site_transactions as updater

class AnalysisTests(unittest.TestCase):
    def setup_feed(self, root):
        store, data=root/'archive',root/'site-data';data.mkdir()
        catalog={'schemaVersion':1,'candidates':[{'id':'x','code':'11170','n':'X','tier':'core','focus':'59',
                 'profiles':[{'id':'59','lo':59,'hi':60,'exactAreas':[59.9]}]}]}
        (data/'candidate-catalog.json').write_text(json.dumps(catalog),encoding='utf-8')
        def item(month,cancel=''):
            return dict(aptNm='X',sggCd='11170',dealYear=month[:4],dealMonth=str(int(month[4:])),dealDay='2',
                        excluUseAr='59.9',floor='1',dealAmount='10,000',cdealType=cancel)
        archive.collect(store,date(2026,10,30),lambda c,m:[item(m),item(m),item(m,'O')],jobs=[('11170','202001'),('11170','202501')])
        rows=[updater.normalize(item('201901'),'11170','201901',{('11170','x'):'X'},'2026-10-31')]
        rows.extend([updater.normalize(item('202501'),'11170','202501',{('11170','x'):'X'},'2026-10-31')]*2)
        seed={'asOf':'2026-10-31','contractCutoff':'2026-10-30','automaticCoverage':[{'region':'11170','month':'202501'}],
              'trackedTargets':[{'code':'11170','n':'X'}]}
        (data/'latest.json').write_text(json.dumps(updater.bundle_data(seed,rows)),encoding='utf-8')
        return store,data

    def test_archive_replacement_and_fresh_multiset_preserve_cancellations(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,data=self.setup_feed(Path(tmp))
            result=analysis.build(store,data,'archive-sha')
            rows=list(csv.DictReader(io.StringIO(result['transactionsCsv'])))
            self.assertEqual(len(rows),6) # pre2020 + archive3 + fresh2; old2025 archive was replaced.
            self.assertEqual(result['cancelledRows'],1)
            self.assertEqual(result['includedRows'],5)
            self.assertEqual(sum(r['거래일'].startswith('2025') for r in rows),2)
            self.assertEqual(hashlib.sha256(result['transactionsCsv'].encode()).hexdigest(),result['csvSha256'])

    def test_corrupt_archive_keeps_previous_analysis(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,data=self.setup_feed(Path(tmp));analysis.build(store,data)
            before=(data/'analysis.json').read_bytes()
            (store/'11170/202001.json.gz').write_bytes(b'broken')
            with self.assertRaises(archive.CollectionError):analysis.build(store,data)
            self.assertEqual(before,(data/'analysis.json').read_bytes())

    def test_missing_core_type_is_not_fabricated(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,data=self.setup_feed(Path(tmp));catalog=json.loads((data/'candidate-catalog.json').read_text())
            catalog['candidates'][0]['profiles'][0].update(lo=72,hi=73)
            (data/'candidate-catalog.json').write_text(json.dumps(catalog))
            with self.assertRaises(updater.CollectionError):analysis.build(store,data)

    def test_verified_catalog_preserves_original_and_actual_types(self):
        catalog=json.loads((updater.ROOT/'site-data/candidate-catalog.json').read_text(encoding='utf-8'))
        c=next(c for c in catalog['candidates'] if c.get('n')=='동탄역센트럴자이(A-10)')
        self.assertEqual(c['focus'],'72');self.assertNotIn('59',[p['id'] for p in c['profiles']])
        c=next(c for c in catalog['candidates'] if c.get('n')=='신흥역하늘채랜더스원2단지')
        self.assertIn(51.89,next(p['exactAreas'] for p in c['profiles'] if p['id']=='51'))
        self.assertEqual(sum(bool(c.get('n')) for c in catalog['candidates']),37)
        self.assertEqual(sum(c['tier']=='core' for c in catalog['candidates']),11)

if __name__=='__main__':unittest.main()

"""Build the expanded Site feed from validated archive + current full-month refresh."""
import argparse
import csv
from datetime import date, datetime
import gzip
import hashlib
import io
import json
import os
from pathlib import Path

import archive_transactions as archive
from update_site_transactions import COLS, CollectionError, KST, cancelled, norm, normalize, load_previous

ROOT = Path(__file__).resolve().parents[1]
MAX_FEED = 20 * 1024 * 1024

def build(directory, data_dir, archive_commit=''):
    manifest = archive.load(directory)
    catalog = json.loads((data_dir/'candidate-catalog.json').read_text(encoding='utf-8'))
    meta, current = load_previous(data_dir)
    targets = {(c['code'], norm(c['n'])): c['n'] for c in catalog['candidates'] if c.get('n') and c.get('code')}
    coverage = {(c['region'], c['month']) for c in meta.get('automaticCoverage', [])}
    refreshed = {(t['code'], norm(t['n'])) for t in meta.get('trackedTargets', [])}
    if not refreshed:  # Older feed: infer only observed names, never assume new targets were refreshed.
        refreshed = {(r['지역코드'], norm(r['단지명'])) for r in current}
    cutoff = meta['contractCutoff']
    rows = [r for r in current if r['거래일'] < archive.START+'-01'
            and (r['지역코드'], norm(r['단지명'])) in targets]
    # Each archived partition replaces the historical month multiset; no deduplication.
    for key, entry in sorted(manifest['partitions'].items()):
        code, month = key.split('/')
        payload = json.loads(gzip.decompress((directory/(key+'.json.gz')).read_bytes()))
        for item in payload['records']:
            target = (code, norm(item.get('aptNm')))
            if target in refreshed and (code, month) in coverage:
                continue
            row = normalize(item, code, month, targets, entry['checkedAt'][:10])
            if row and row['거래일'] <= cutoff:
                row['출처'] = '국토부 상세 API 아카이브 · 확인 '+entry['checkedAt'][:10]
                rows.append(row)
    rows.extend(r for r in current if r['거래일'] >= archive.START+'-01'
                and (r['지역코드'], norm(r['단지명'])) in targets
                and (r['지역코드'], r['거래일'][:7].replace('-','')) in coverage
                and (r['지역코드'], norm(r['단지명'])) in refreshed)
    rows.sort(key=lambda r:(r['거래일'], r['지역코드'], r['단지명'], float(r['전용면적']), int(r['거래금액(만원)'])))
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=COLS, lineterminator='\n')
    writer.writeheader(); writer.writerows(rows)
    text = stream.getvalue()
    bundle = {'schemaVersion':1,'asOf':meta['asOf'],'contractCutoff':cutoff,'generatedAt':datetime.now(KST).isoformat(),
              'catalog':catalog,'transactionsCsv':text,'csvSha256':hashlib.sha256(text.encode()).hexdigest(),
              'rawRows':len(rows),'includedRows':sum(not cancelled(r) for r in rows),
              'cancelledRows':sum(cancelled(r) for r in rows),
              'archive':{'commit':archive_commit,'completedPartitions':manifest['completedPartitions'],
                         'expectedPartitions':manifest['expectedPartitions'],'backfillComplete':manifest['backfillComplete'],
                         'checkedAtMin':min(e['checkedAt'] for e in manifest['partitions'].values()),
                         'checkedAtMax':max(e['checkedAt'] for e in manifest['partitions'].values())},
              'refreshedTargets':len(refreshed),'collectionRunUrl':meta.get('collectionRunUrl'),
              'note':'해제 원자료 보존·분석 제외, 직거래 포함, 별도 계약 중복제거 없음. 2020년부터 아카이브, 2025년부터 일일 전체월 갱신 우선. 최근 30일 신고 지연. KB·호가 별도.'}
    # Every focal candidate must have its exact observed focus type. No fabricated zero series.
    for candidate in catalog['candidates']:
        if candidate['tier'] != 'core': continue
        p=next(p for p in candidate['profiles'] if p['id']==candidate['focus'])
        if not any(r['지역코드']==candidate['code'] and r['단지명']==candidate['n']
                   and p['lo']<=float(r['전용면적'])<p['hi'] and not cancelled(r) for r in rows):
            raise CollectionError('Missing verified core complex/area')
    data = json.dumps(bundle,ensure_ascii=False,separators=(',',':')).encode()
    if len(data)>MAX_FEED: raise CollectionError('Analysis feed size budget exceeded')
    output = data_dir/'analysis.json'
    temp = output.with_suffix('.tmp'); temp.write_bytes(data); temp.replace(output)
    return bundle

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--archive-dir',type=Path,required=True)
    parser.add_argument('--data-dir',type=Path,default=ROOT/'site-data')
    parser.add_argument('--archive-commit',default='')
    args=parser.parse_args()
    try:
        result=build(args.archive_dir,args.data_dir,args.archive_commit)
        print(f"Analysis feed verified: {result['rawRows']} raw / {result['includedRows']} valid")
    except Exception:
        print('Analysis feed build failed; do not publish either Site bundle.')
        raise SystemExit(1) from None

"""Publish complete, validated MOLIT district-month replacements; never log keys."""
import argparse
import concurrent.futures
import copy
import csv
import hashlib
import gzip
import io
import json
import os
from pathlib import Path
import re
import statistics
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode, unquote
from urllib.request import urlopen
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
API = 'https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev/getRTMSDataSvcAptTradeDev'
KST = timezone(timedelta(hours=9))
COLS = ['지역코드','단지명','거래일','전용면적','층','거래금액(만원)','법정동','지번','동','등기일자','거래유형','해제사유발생일','기타_cdealDay','해제여부','매수자구분','매도자구분','출처']

class CollectionError(RuntimeError):
    pass

def norm(value):
    return re.sub(r'[^0-9A-Za-z가-힣]', '', value or '').lower()

def clean(value):
    value = str(value or '').strip()
    return '' if value in ('-', 'nan', 'None') else value

def cancelled(row):
    return bool(clean(row.get('해제사유발생일')) or clean(row.get('기타_cdealDay')) or clean(row.get('해제여부')).upper() in ('Y','O','1'))

def months(start, cutoff):
    current = date.fromisoformat(start + '-01')
    while current <= cutoff:
        yield current.strftime('%Y%m')
        current = date(current.year + current.month // 12, current.month % 12 + 1, 1)

def fetch_page(key, code, month, page, per_page=5000):
    query = urlencode({'serviceKey': unquote(key), 'LAWD_CD': code, 'DEAL_YMD': month,
                       'pageNo': page, 'numOfRows': per_page})
    for attempt in range(3):
        try:
            with urlopen(API + '?' + query, timeout=45) as response:
                root = ET.fromstring(response.read())
            if root.findtext('.//resultCode', '') not in ('00','000','0','200'):
                raise CollectionError(f'{code} {month} page {page}: API rejected request')
            total = root.findtext('.//totalCount')
            if total is None or not total.isdigit():
                raise CollectionError(f'{code} {month}: missing totalCount')
            items = [{node.tag: clean(node.text) for node in item} for item in root.findall('.//item')]
            return int(total), items
        except CollectionError:
            raise
        except Exception:
            if attempt == 2:
                raise CollectionError(f'{code} {month} page {page}: network/XML failure') from None
            time.sleep(2 ** attempt)

def fetch_month(key, code, month, fetcher=fetch_page):
    total, rows = fetcher(key, code, month, 1)
    fingerprint = lambda chunk: hashlib.sha256(json.dumps(chunk, sort_keys=True).encode()).hexdigest()
    seen, page = {fingerprint(rows)}, 1
    while len(rows) < total:
        if not rows:
            raise CollectionError(f'{code} {month}: empty first page')
        page += 1
        if page > 100:
            raise CollectionError(f'{code} {month}: page limit exceeded')
        next_total, more = fetcher(key, code, month, page)
        if next_total != total or not more:
            raise CollectionError(f'{code} {month}: incomplete/changing result count')
        signature = fingerprint(more)
        if signature in seen:
            raise CollectionError(f'{code} {month}: repeated page')
        seen.add(signature)
        rows.extend(more)
    if len(rows) != total:
        raise CollectionError(f'{code} {month}: totalCount mismatch')
    return rows  # Separate identical-looking contracts must remain separate.

def normalize(item, code, month, targets, as_of):
    key = (code, norm(item.get('aptNm')))
    if key not in targets:
        return None
    try:
        day = date(int(item['dealYear']), int(item['dealMonth']), int(item['dealDay']))
        if day.strftime('%Y%m') != month:
            raise ValueError()
        area = float(item['excluUseAr'])
        amount = int(item['dealAmount'].replace(',', '').strip())
        floor = int(item['floor'])
        if not 0 < area < 1000 or amount <= 0:
            raise ValueError()
    except (KeyError, ValueError, TypeError):
        raise CollectionError(f'{code} {month}: invalid target record') from None
    return dict(zip(COLS, [code, targets[key], day.isoformat(), item['excluUseAr'], str(floor), str(amount),
        item.get('umdNm',''), item.get('jibun',''), item.get('aptDong',''), item.get('rgstDate',''),
        item.get('dealingGbn') or item.get('reqGbn') or '미기재',
        item.get('cancelDealDay',''), item.get('cdealDay',''), item.get('cdealType',''),
        item.get('buyerGbn',''), item.get('slerGbn',''), f'국토부 상세 API · GitHub 자동수집 {as_of}']))

def replace_months(old, fresh, covered):
    for code, month in covered:
        if any(r['지역코드'] == code and r['거래일'][:7].replace('-','') == month for r in old):
            if not any(r['지역코드'] == code and r['거래일'][:7].replace('-','') == month for r in fresh):
                raise CollectionError(f'{code} {month}: all prior target records disappeared')
    retained = [r for r in old if (r['지역코드'], r['거래일'][:7].replace('-','')) not in covered]
    return sorted(retained + fresh, key=lambda r:(r['거래일'],r['단지명'],float(r['전용면적']),int(r['거래금액(만원)'])))

def summarize(seed, rows, as_of, cutoff, coverage):
    meta = copy.deepcopy(seed)
    valid = [r for r in rows if not cancelled(r)]
    recent_start = date(cutoff.year + (cutoff.month - 4) // 12, (cutoff.month - 4) % 12 + 1, 1).isoformat()
    for candidate in meta['complexes'] + meta.get('goalComparisons', []):
        records = [r for r in valid if r['지역코드'] == candidate['code'] and r['단지명'] == candidate['n']
                   and candidate['lo'] <= float(r['전용면적']) < candidate['hi']]
        if not records:
            raise CollectionError('Missing focal complex/area: ' + candidate['n'])
        last = records[-1]['거래일']
        last_rows = [r for r in records if r['거래일'] == last]
        recent = [r for r in records if r['거래일'] >= recent_start]
        candidate.update(count=len(records), recentCount=len(recent), firstDate=records[0]['거래일'],
                         lastDate=last, lastPrices=sorted({int(r['거래금액(만원)']) for r in last_rows}),
                         lastTrades=last_rows, recentTrades=records[-8:][::-1],
                         recentMedian=statistics.median(int(r['거래금액(만원)']) for r in recent) if recent else None)
    meta.update(asOf=as_of, contractCutoff=cutoff.isoformat(), rawRows=len(rows), includedRows=len(valid),
                cancelledRows=len(rows)-len(valid), kbAsOf=seed.get('kbAsOf', seed['asOf']),
                source='국토부 상세 API → GitHub 매일 자동수집 · 2025년 이전은 기존 보존 자료',
                autoUpdated=True, automaticCoverage=coverage, baselineAsOf='2026-10-21',
                note='해제 제외·직거래 포함. 2025년부터 지역·월 전체 재조회, 동일 조건의 별도 계약 보존. 최근 30일은 신고 지연으로 불완전. KB시세·호가는 자동갱신 대상 아님.')
    meta['baselineReconciliation'] = meta.pop('reconciliation', meta.get('baselineReconciliation', {}))
    meta['collectorCodeCommit'] = os.environ.get('GITHUB_SHA', '')
    meta['collectionRunUrl'] = ('https://github.com/' + os.environ['GITHUB_REPOSITORY'] + '/actions/runs/' + os.environ['GITHUB_RUN_ID']) if os.environ.get('GITHUB_RUN_ID') else None
    return meta

def bundle_data(meta, rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=COLS, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    text = stream.getvalue()
    return {'schemaVersion': 1, 'generatedAt': datetime.now(KST).isoformat(),
            'csvSha256': hashlib.sha256(text.encode('utf-8')).hexdigest(),
            'snapshot': meta, 'transactionsCsv': text}

def load_previous(directory):
    path = directory/'latest.json'
    if path.exists():
        bundle = json.loads(path.read_text(encoding='utf-8'))
        text = bundle['transactionsCsv']
        if hashlib.sha256(text.encode('utf-8')).hexdigest() != bundle['csvSha256']:
            raise CollectionError('Existing bundle checksum mismatch')
        return bundle['snapshot'], list(csv.DictReader(io.StringIO(text)))
    meta = json.loads(gzip.decompress((directory/'seed-market.json.gz').read_bytes()).decode('utf-8'))
    text = gzip.decompress((directory/'seed-transactions.csv.gz').read_bytes()).decode('utf-8-sig')
    return meta, list(csv.DictReader(io.StringIO(text)))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--refresh-start', default='2025-01')
    parser.add_argument('--data-dir', type=Path, default=ROOT/'site-data')
    options = parser.parse_args()
    key = os.environ.get('MOLIT_SERVICE_KEY', '').strip()
    if not key:
        raise CollectionError('MOLIT_SERVICE_KEY is not configured')
    today = datetime.now(KST).date()
    cutoff = today - timedelta(days=1)
    seed, old = load_previous(options.data_dir)
    targets = {(c['code'], norm(c['n'])):c['n'] for c in seed['complexes']}
    jobs = [(code, month) for code in sorted({c['code'] for c in seed['complexes']}) for month in months(options.refresh_start, cutoff)]
    fresh, coverage = [], []
    def collect(job):
        code, month = job
        items = fetch_month(key, code, month)
        normalized = [normalize(item,code,month,targets,today.isoformat()) for item in items]
        normalized = [r for r in normalized if r and r['거래일'] <= cutoff.isoformat()]
        print(f'{code} {month}: total {len(items)}, target {len(normalized)}', flush=True)
        return normalized, {'region':code,'month':month,'apiTotalRows':len(items),'targetRows':len(normalized)}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        for records, info in pool.map(collect, jobs):
            fresh.extend(records)
            coverage.append(info)
    rows = replace_months(old, fresh, set(jobs))
    meta = summarize(seed, rows, today.isoformat(), cutoff, coverage)
    if meta['includedRows'] < seed['includedRows'] * .85:
        raise CollectionError('More than 15% of valid records disappeared; retaining previous publication')
    bundle = bundle_data(meta, rows)
    output = options.data_dir/'latest.json'
    temp = options.data_dir/'latest.json.tmp'
    temp.write_text(json.dumps(bundle, ensure_ascii=False, separators=(',',':')), encoding='utf-8', newline='\n')
    temp.replace(output)
    print(f'Published bundle: {meta["includedRows"]} valid / {meta["rawRows"]} raw, as of {meta["asOf"]}')

if __name__ == '__main__':
    try:
        main()
    except CollectionError as error:
        print('Collection failed: ' + str(error))
        raise SystemExit(1) from None

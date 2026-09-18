"""Resumable, evidence-preserving municipal PDCom source discovery.

A bounded crawl's empty result never proves the absence of a plan. Candidates
need document/version/coverage review before becoming extraction inputs.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import unicodedata
from urllib.parse import urljoin, urlsplit, unquote
import uuid

from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import Json, RealDictCursor
import requests

DIRECTORY = 'https://www.ucv.ch/annuaire/recherche-par-localite'
PLAN = re.compile(r'pdcom|plan[s]?[-_ /]+directeur[s]?[-_ /]+communal|dossier[-_ /]+directeur', re.I)
NAV = re.compile(r'urbanis|amenagement|planific|plan[-_ ]directeur|dossier[-_ ]directeur|pdcom|territoire|reglement|documents|construction', re.I)


def normalize(value):
    value = re.sub(r'\s*\(VD\)', '', value, flags=re.I)
    return re.sub(r'[^a-z0-9]', '', ''.join(c for c in unicodedata.normalize('NFKD', value.lower()) if not unicodedata.combining(c)))


def public_url(url):
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password or parsed.port not in (None, 80, 443):
        raise ValueError('invalid_public_url')
    if any(not ipaddress.ip_address(item[4][0]).is_global for item in socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)):
        raise ValueError('non_public_address')
    return url


def fetch_html(url):
    for _ in range(5):
        public_url(url)
        with requests.get(url, timeout=(8, 12), stream=True, allow_redirects=False,
                          headers={'User-Agent': 'Pixxels-PDCom-source-discovery/1.0'}) as response:
            if response.is_redirect:
                url = urljoin(url, response.headers['Location'])
                continue
            response.raise_for_status()
            if 'html' not in response.headers.get('Content-Type', '').lower():
                raise ValueError('not_html')
            data = bytearray()
            for chunk in response.iter_content(65536):
                data.extend(chunk)
                if len(data) > 4_000_000:
                    raise ValueError('html_size_limit')
            return url, BeautifulSoup(bytes(data), 'html.parser'), hashlib.sha256(data).hexdigest()
    raise ValueError('redirect_limit')


def page_links(page, diagnostics):
    """Read published i-web table JSON as inert data, with bounded expansion."""
    for link in page.select('a[href]'):
        yield link, ''
    rows_left = 1000
    bytes_left = 1_000_000
    for table in page.select('table[data-entities]'):
        raw = table['data-entities']
        if len(raw.encode('utf-8')) > min(500_000, bytes_left):
            diagnostics.append({'stage': 'embedded_links', 'reason': 'embedded_data_size_limit'})
            continue
        bytes_left -= len(raw.encode('utf-8'))
        try:
            payload = json.loads(raw)
        except (ValueError, RecursionError):
            diagnostics.append({'stage': 'embedded_links', 'reason': 'invalid_embedded_json'})
            continue
        rows = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            continue
        if len(rows) > rows_left:
            diagnostics.append({'stage': 'embedded_links', 'reason': 'embedded_row_limit'})
        selected = rows[:rows_left]
        rows_left -= len(selected)
        for row in selected:
            if not isinstance(row, dict):
                continue
            name = row.get('name', '')
            context = BeautifulSoup(name, 'html.parser').get_text(' ', strip=True)[:500] if isinstance(name, str) else ''
            for key in ('name', '_downloadBtn'):
                fragment = row.get(key)
                if not isinstance(fragment, str):
                    continue
                for link in BeautifulSoup(fragment, 'html.parser').select('a[href]'):
                    yield link, context


def directory_seeds(soup):
    result = {}
    for link in soup.select('a[href]'):
        if '/commune/' in link['href']:
            name = unquote(link['href'].rstrip('/').rsplit('/', 1)[-1])
            key = normalize(name)
            if key in result and result[key] != urljoin(DIRECTORY, link['href']):
                raise ValueError('ambiguous_directory_name')
            result[key] = urljoin(DIRECTORY, link['href'])
    if len(result) < 290:
        raise ValueError('directory_incomplete')
    return result


def discover(row, page_limit=12):
    result = {'commune_bfs': row['commune_bfs'], 'directory_url': row['directory_url'],
              'pages': [], 'candidates': [], 'errors': [], 'website_url': None}
    if not row['directory_url']:
        result['errors'].append({'stage': 'seed', 'error': 'directory_name_unmatched'})
        return result
    try:
        detail_url, soup, sha = fetch_html(row['directory_url'])
        # UCV's official website field displays the domain itself. Exclude footer,
        # social and agency links whose visible text is not their domain.
        websites = [a['href'] for a in soup.select('a[href]')
                    if a.get_text(strip=True).lower().startswith('www.')
                    and urlsplit(a['href']).hostname == a.get_text(strip=True).lower().rstrip('/')]
        websites = list(dict.fromkeys(websites))
        result['directory_sha256'] = sha
        if len(websites) != 1:
            raise ValueError('official_website_missing_or_ambiguous')
        home_url, home, home_sha = fetch_html(websites[0])
        result['website_url'] = home_url
        host = urlsplit(home_url).hostname
        queue = [(home_url, home, home_sha)]
        seen = set()
        candidates = {}
        while queue and len(seen) < page_limit:
            url, page, page_sha = queue.pop(0)
            if url in seen:
                continue
            seen.add(url)
            if page is None:
                try:
                    final, page, page_sha = fetch_html(url)
                    if urlsplit(final).hostname != host:
                        raise ValueError('cross_host_page_redirect')
                    url = final
                except Exception as exc:
                    result['errors'].append({'url': url, 'error': type(exc).__name__})
                    continue
            result['pages'].append({'url': url, 'sha256': page_sha})
            base = page.find('base',href=True)
            base_url = urljoin(url,base['href']) if base else url
            embedded_diagnostics = []
            heading = page.find('h1')
            heading_text = heading.get_text(' ', strip=True) if heading else ''
            page_is_plan = bool(PLAN.search(unquote(url)) or PLAN.search(heading_text))
            for link, context in page_links(page, embedded_diagnostics):
                target = urljoin(base_url, link['href']).split('#')[0]
                parsed = urlsplit(target)
                if parsed.scheme not in ('http', 'https') or parsed.username or parsed.password:
                    continue
                label = link.get_text(' ', strip=True)
                if context and context not in label:
                    label = context + ' ' + label
                label = label[:500]
                text = unicodedata.normalize('NFKD', unquote(label+' '+parsed.path)).encode('ascii', 'ignore').decode()
                is_download = parsed.path.lower().endswith('.pdf') or bool(re.fullmatch(r'/_doc/\d+', parsed.path))
                if PLAN.search(text) or (page_is_plan and is_download):
                    candidates[target] = {'source_url': target, 'evidence_url': url, 'link_text': label,
                                          'kind': 'pdf' if parsed.path.lower().endswith('.pdf') else 'landing',
                                          'review_status': 'pending'}
                if parsed.hostname == host and not is_download and not re.search(r'\.(pdf|zip|docx?|xlsx?|jpg|png)$', parsed.path, re.I) and NAV.search(text) and target not in seen and all(item[0] != target for item in queue):
                    queue.append((target, None, None))
            result['errors'].extend({'url': url, **item} for item in embedded_diagnostics)
            queue.sort(key=lambda item: not bool(PLAN.search(unquote(item[0]))))
        result['candidates'] = list(candidates.values())
        result['crawl_limit_reached'] = bool(queue)
    except Exception as exc:
        result['errors'].append({'stage': 'website', 'error': type(exc).__name__,
                                 'reason': str(exc) if isinstance(exc, ValueError) else None})
    return result


def run(args):
    conn = psycopg2.connect(os.environ['RE_LLM_DB_URL'], connect_timeout=15)
    operation = str(uuid.uuid4())
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    lock_conn = None
    try:
        # A dedicated transaction holds the pipeline lock across per-commune
        # commits. Session locks can survive pooled-client disconnects.
        lock_conn = psycopg2.connect(os.environ['RE_LLM_DB_URL'], connect_timeout=15)
        with lock_conn.cursor() as lock_cur:
            lock_cur.execute('SELECT pg_try_advisory_xact_lock(572500300)')
            acquired = lock_cur.fetchone()[0]
        if not acquired:
            with conn.cursor() as cur:
                cur.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_communes WHERE is_current')
                count = cur.fetchone()[0]
            return {'operation_id':operation,'processed':0,'queue_counts':{'current_communes':count},'skipped':'discovery_already_running','coverage_complete':False}
        _, directory, directory_sha = fetch_html(DIRECTORY)
        seeds = directory_seeds(directory)
        with conn, conn.cursor() as cur:
            cur.execute('SELECT commune_bfs,commune_name FROM bronze_ch.vd_pdcom_communes WHERE is_current')
            for bfs, name in cur.fetchall():
                cur.execute('''INSERT INTO bronze_ch.vd_pdcom_discovery_queue(commune_bfs,directory_url)
                VALUES (%s,%s) ON CONFLICT(commune_bfs) DO UPDATE SET directory_url=excluded.directory_url''', (bfs, seeds.get(normalize(name))))
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT q.* FROM bronze_ch.vd_pdcom_discovery_queue q JOIN bronze_ch.vd_pdcom_communes c USING(commune_bfs)
            WHERE c.is_current AND (q.next_attempt_at<=now() OR
            (%s AND q.attempt_count<2 AND q.status IN ('manual_search_required','blocked','candidates_found')))
            ORDER BY q.attempt_count,q.commune_bfs LIMIT %s''', (getattr(args,'backlog',False),args.limit))
            rows = cur.fetchall()
        results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            pending = {executor.submit(discover, row, args.pages): row for row in rows}
            for future in as_completed(pending):
                result = future.result()
                bfs = result['commune_bfs']
                status = 'candidates_found' if result['candidates'] else ('blocked' if not result['website_url'] else 'manual_search_required')
                # Exhausted bounded crawl is manual review, not verified absence.
                retry_days = 7 if status == 'blocked' else (30 if status == 'manual_search_required' else 365)
                with conn, conn.cursor() as cur:
                    cur.execute('''INSERT INTO bronze_ch.vd_pdcom_discovery_attempts(operation_id,commune_bfs,outcome,evidence)
                    VALUES(%s,%s,%s,%s)''', (operation, bfs, status, Json(result)))
                    for candidate in result['candidates']:
                        cur.execute('''INSERT INTO bronze_ch.vd_pdcom_source_candidates(commune_bfs,source_url,evidence_url,link_text,kind)
                        VALUES(%s,%s,%s,%s,%s) ON CONFLICT(commune_bfs,source_url) DO UPDATE SET
                        last_seen_at=now(),evidence_url=excluded.evidence_url,link_text=excluded.link_text''',
                        (bfs,candidate['source_url'],candidate['evidence_url'],candidate['link_text'],candidate['kind']))
                    cur.execute('''UPDATE bronze_ch.vd_pdcom_discovery_queue SET website_url=%s,status=%s,attempt_count=attempt_count+1,
                    last_attempt_at=now(),next_attempt_at=now()+(%s * interval '1 day') WHERE commune_bfs=%s''', (result['website_url'],status,retry_days,bfs))
                results.append(result)
                (output/f'{bfs}-discovery.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
                print(json.dumps({'commune_bfs':bfs,'outcome':status,'candidates':len(result['candidates'])}), flush=True)
        with conn, conn.cursor() as cur:
            cur.execute('''SELECT q.status,count(*) FROM bronze_ch.vd_pdcom_discovery_queue q JOIN bronze_ch.vd_pdcom_communes c USING(commune_bfs) WHERE c.is_current GROUP BY 1''')
            counts = dict(cur.fetchall())
        report = {'operation_id':operation,'processed':len(results),'directory_sha256':directory_sha,
                  'queue_counts':counts,'candidate_count':sum(len(r['candidates']) for r in results),
                  'coverage_complete':False}
        (output/'discovery-report.json').write_text(json.dumps(report,indent=2))
        print(json.dumps(report),flush=True)
        return report
    finally:
        try:
            if lock_conn is not None:
                try:
                    lock_conn.rollback()
                finally:
                    lock_conn.close()
        finally:
            conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit',type=int,default=30)
    parser.add_argument('--workers',type=int,default=4)
    parser.add_argument('--pages',type=int,default=12)
    parser.add_argument('--output',type=Path,default=Path('vd-pdcom-output'))
    parser.add_argument('--monitor',action='store_true')
    parser.add_argument('--backlog',action='store_true',help='Run one deeper second pass before normal recurrence')
    args = parser.parse_args()
    if args.monitor:
        from parser import Monitor
        monitor = Monitor()
        monitor.begin(uuid.uuid4())
        try:
            report = run(args)
            monitor.finish({'current_communes':sum(report['queue_counts'].values()),
                            'discovery':report,'coverage_complete':False},True)
        except Exception as exc:
            monitor.finish({'error_type':type(exc).__name__,'coverage_complete':False},False)
            raise
    else:
        run(args)

"""Fetch the full text of SHAB commercial-register (HR) notices into re-LLM bronze_ch.shab_hr_notice_texts.
Resumable and idempotent: only notices indexed in bronze_ch.amtsblatt_publications (pipelines/amtsblattportal) and not
fetched yet, newest first. Env: RE_LLM_DB_URL, SHAB_CANTONS (default GE,VD), SHAB_SINCE (YYYY-MM-DD, default 30 days ago), SHAB_RATE."""
import datetime, json, os, sys, time, threading, urllib.request, urllib.error, xml.etree.ElementTree as ET
import concurrent.futures as cf
import psycopg2, psycopg2.extras

CANTONS = os.environ.get('SHAB_CANTONS', 'GE,VD').split(',')
SINCE = os.environ.get('SHAB_SINCE') or str(datetime.date.today() - datetime.timedelta(days=30))
RATE = float(os.environ.get('SHAB_RATE', '20'))
URI = os.environ['RE_LLM_DB_URL']
API = 'https://www.amtsblattportal.ch/api/v1/publications/{}/xml'

_lock, _next = threading.Lock(), [0.0]
def throttle():
    with _lock:
        now = time.time(); wait = _next[0] - now
        _next[0] = max(now, _next[0]) + 1.0 / RATE
    if wait > 0: time.sleep(wait)

def text(node, path):
    el = node.find(path)
    return el.text if el is not None else None

def fetch(pid):
    for attempt in range(4):
        throttle()
        try:
            with urllib.request.urlopen(API.format(pid), timeout=30) as r:
                root = ET.fromstring(r.read())
            meta, content = root.find('meta'), root.find('content')
            return dict(publication_id=pid, publication_number=text(meta, 'publicationNumber'), publication_date=text(meta, 'publicationDate'),
                        sub_rubric=text(meta, 'subRubric'), cantons=(text(meta, 'cantons') or '').split(',') if text(meta, 'cantons') else None,
                        language=text(meta, 'language'), journal_number=text(content, 'journalNumber') if content is not None else None,
                        journal_date=text(content, 'journalDate') if content is not None else None,
                        publication_text=text(content, 'publicationText') if content is not None else None, http_status=200)
        except urllib.error.HTTPError as e:
            if e.code in (404, 410): return dict(publication_id=pid, http_status=e.code)
            time.sleep(2 ** attempt)
        except Exception:
            time.sleep(2 ** attempt)
    return None  # retried later

COLS = ['publication_id', 'publication_number', 'publication_date', 'sub_rubric', 'cantons', 'language', 'journal_number', 'journal_date', 'publication_text', 'http_status']

def main():
    conn = psycopg2.connect(URI, options='-c statement_timeout=300000'); conn.autocommit = True
    done = 0; t0 = time.time()
    while True:
        with conn.cursor() as c:
            c.execute("""SELECT p.id FROM bronze_ch.amtsblatt_publications p
                         WHERE p.rubric='HR' AND p.cantons && %s::text[] AND p.publication_date >= %s
                           AND NOT EXISTS (SELECT 1 FROM bronze_ch.shab_hr_notice_texts t WHERE t.publication_id=p.id)
                         ORDER BY p.publication_date DESC LIMIT 2000""", (CANTONS, SINCE))
            ids = [str(r[0]) for r in c.fetchall()]
        if not ids: break
        with cf.ThreadPoolExecutor(8) as ex:
            rows = [r for r in ex.map(fetch, ids) if r]
        with conn.cursor() as c:
            psycopg2.extras.execute_values(c, f"INSERT INTO bronze_ch.shab_hr_notice_texts ({','.join(COLS)}) VALUES %s ON CONFLICT (publication_id) DO NOTHING",
                                           [tuple(r.get(k) for k in COLS) for r in rows])
        done += len(rows)
        print(f'{time.strftime("%H:%M:%S")} fetched {done} ({done / (time.time() - t0):.1f}/s), last batch {len(rows)}/{len(ids)}', flush=True)
        if len(rows) == 0: break  # API failing for every notice of the batch: stop, the next run retries
    print('complete', done, flush=True)

if __name__ == '__main__':
    main()

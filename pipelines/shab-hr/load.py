"""Parse fetched SHAB HR notices (bronze_ch.shab_hr_notice_texts) into silver_ch.shab_hr_* . Incremental: notices not yet
parsed at the current PARSE_VERSION. Re-running after a parser change (bump PARSE_VERSION) re-parses everything."""
import json, os, re, sys, unicodedata, time
import psycopg2, psycopg2.extras
sys.path.insert(0, __file__.rsplit('/', 1)[0])
from parse import parse

PARSE_VERSION = 1
URI = os.environ['RE_LLM_DB_URL']

def norm(s):
    s = unicodedata.normalize('NFKD', s or '').encode('ascii', 'ignore').decode().lower()
    return re.sub(r'[^a-z]+', ' ', s).strip()

def main():
    conn = psycopg2.connect(URI, options='-c statement_timeout=600000'); conn.autocommit = False
    total = 0
    while True:
        with conn.cursor() as c:
            c.execute("""SELECT t.publication_id, t.publication_date, t.cantons, t.sub_rubric, t.publication_text FROM bronze_ch.shab_hr_notice_texts t
                         WHERE t.http_status=200 AND t.publication_text IS NOT NULL
                           AND NOT EXISTS (SELECT 1 FROM silver_ch.shab_hr_company_events e WHERE e.publication_id=t.publication_id AND e.parse_version=%s)
                         LIMIT 5000""", (PARSE_VERSION,))
            rows = c.fetchall()
        if not rows: break
        ev, roles, links = [], [], []
        for pid, d, cant, sr, text in rows:
            p = parse(text, sr); co = p['company']
            ev.append((pid, d, cant, co.get('uid'), co.get('name'), co.get('seat'), co.get('address'), co.get('care_of'), co.get('event'),
                       co.get('new_name'), co.get('dissolved'), co.get('capital_chf'), PARSE_VERSION))
            for r in p['roles']:
                roles.append((pid, d, co.get('uid'), co.get('name'), r['name'], norm(r['name']), r['origin'], r['domicile'], r['role'], r['signature'], r['action'], PARSE_VERSION))
            for l in p['company_links']:
                links.append((pid, d, co.get('uid'), l['uid'], l['name'], l['kind'], PARSE_VERSION))
        ids = [r[0] for r in rows]
        with conn.cursor() as c:
            c.execute("DELETE FROM silver_ch.shab_hr_person_roles WHERE publication_id = ANY(%s::uuid[])", (ids,))
            c.execute("DELETE FROM silver_ch.shab_hr_company_links WHERE publication_id = ANY(%s::uuid[])", (ids,))
            psycopg2.extras.execute_values(c, """INSERT INTO silver_ch.shab_hr_company_events (publication_id, publication_date, cantons, uid, company_name, seat, address, care_of, event, new_name, dissolved, capital_chf, parse_version)
                VALUES %s ON CONFLICT (publication_id) DO UPDATE SET publication_date=excluded.publication_date, cantons=excluded.cantons, uid=excluded.uid, company_name=excluded.company_name,
                seat=excluded.seat, address=excluded.address, care_of=excluded.care_of, event=excluded.event, new_name=excluded.new_name, dissolved=excluded.dissolved,
                capital_chf=excluded.capital_chf, parse_version=excluded.parse_version, parsed_at=now()""", ev)
            if roles: psycopg2.extras.execute_values(c, "INSERT INTO silver_ch.shab_hr_person_roles (publication_id, publication_date, uid, company_name, person_name, person_name_norm, origin, domicile, role, signature, action, parse_version) VALUES %s", roles)
            if links: psycopg2.extras.execute_values(c, "INSERT INTO silver_ch.shab_hr_company_links (publication_id, publication_date, uid, linked_uid, linked_name, kind, parse_version) VALUES %s", links)
        conn.commit(); total += len(rows)
        print(f'{time.strftime("%H:%M:%S")} parsed {total} notices (+{len(roles)} roles, +{len(links)} links)', flush=True)

if __name__ == '__main__':
    main()

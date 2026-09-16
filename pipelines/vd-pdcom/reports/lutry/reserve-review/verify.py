"""Check raw OCR coverage and mandatory qualifications on Lutry approval."""
import hashlib
import json
from pathlib import Path
P=Path(__file__).resolve().parent
root=P.parents[2]
m=json.loads((P/'ocr-manifest.json').read_text())
expected={(part,page) for part,total in [(1,44),(2,21),(3,21)] for page in range(1,total+1)}
assert len(m['pages'])==86 and {(r['part'],r['pdf_page']) for r in m['pages']}==expected
for r in m['pages']:
    text=(P/r['text_file']).read_text()
    assert len(text)==r['chars'] and hashlib.sha256(text.encode()).hexdigest()==r['sha256']
for s in m['source_pdfs']:
    assert hashlib.sha256((P/f"../source-review/part-{s['part']}.pdf").read_bytes()).hexdigest()==s['sha256']
e=json.loads((P/'approval-exceptions.json').read_text())
assert e['pdf_page']==9 and e['decision_date']=='2000-02-28'
assert [s['name'] for s in e['sectors']]==['Grandchamp','Bossière','situé entre le Châtelard et le Daley']
assert not e['current_resolution_verified'] and not e['geometries_digitized']
reviews=json.loads((root/'document_reviews.json').read_text())
for s in m['source_pdfs'][:2]:
    r=next(r for r in reviews if r['document_id']==s['document_id'])
    assert r['approval_reservations']==e and r['receiver_release']=='not_ready'
g=json.loads((P/'source-gaps.json').read_text());assert g['source_completeness']=='incomplete' and g['receiver_release']=='not_ready'
print('Verified 86 OCR pages and preserved three approval exceptions / two missing annexes.')

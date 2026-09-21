"""Replay byte-specific, page-linked research batches during source persistence.

Only inspection annotations are written. Source approval, geography, commune
completion and receiver eligibility are never advanced by this replay.
"""
import copy
import json
from pathlib import Path
from psycopg2.extras import Json

ROOT = Path(__file__).with_name('reviewed_batches')
ALLOWED_KEYS = {'source_review', 'reviewed_map', 'operational_batch', 'reviewed_operational_metrics'}


def matching_batch(document_id, sha):
    # UUID filenames are selected from a fixed local registry, never caller paths.
    for path in sorted(ROOT.glob('*.json')):
        batch = json.loads(path.read_text())
        if batch['document_id'] == document_id and batch['sha256'] == sha:
            return batch
    return None


def annotate(inspection, page_count, batch):
    if batch['page_count'] != page_count:
        raise ValueError('batch_page_count_mismatch')
    result = copy.deepcopy(inspection)
    pages = {}
    for page in result:
        number = page.get('page_number')
        if number is None:
            continue  # Preserve existing document-level research metadata.
        if type(number) is not int or not 1 <= number <= page_count or number in pages:
            raise ValueError('batch_inspection_pages_invalid')
        pages[number] = page
    if set(pages) != set(range(1, page_count + 1)):
        raise ValueError('batch_inspection_pages_missing')
    seen = set()
    for annotation in batch['page_annotations']:
        number = annotation['page_number']
        fields = annotation['fields']
        if type(number) is not int or number not in pages or number in seen:
            raise ValueError('batch_annotation_page_invalid')
        if not fields or not set(fields) <= ALLOWED_KEYS:
            raise ValueError('batch_annotation_fields_invalid')
        seen.add(number)
        pages[number].update(copy.deepcopy(fields))
    for key, value in batch.get('document_annotations', {}).items():
        if key not in ALLOWED_KEYS:
            raise ValueError('batch_annotation_fields_invalid')
        holders = [item for item in result if item.get('page_number') is None and key in item]
        if len(holders) > 1:
            raise ValueError('batch_document_annotation_ambiguous')
        if holders:
            holders[0][key] = copy.deepcopy(value)
        else:
            result.append({key: copy.deepcopy(value)})
    return result


def apply_batch(conn, document_id, sha):
    batch = matching_batch(document_id, sha)
    if batch is None:
        return False
    with conn, conn.cursor() as c:
        c.execute('SELECT inspection,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s AND sha256=%s FOR UPDATE', (document_id, sha))
        row = c.fetchone()
        if row is None:
            raise ValueError('batch_document_missing')
        updated = annotate(row[0], row[1], batch)
        if updated != row[0]:
            c.execute('UPDATE bronze_ch.vd_pdcom_documents SET inspection=%s WHERE id=%s AND sha256=%s', (Json(updated), document_id, sha))
            if c.rowcount != 1:
                raise ValueError('batch_target_changed')
    return True

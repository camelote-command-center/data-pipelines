"""
Batch-classify all pending knowledge items in re-LLM.
Idempotent. Re-run safely after interruption.
"""
import os, json, time, sys
from datetime import datetime
from pathlib import Path
import anthropic
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL  = os.environ['SUPABASE_RE_LLM_URL']
SUPABASE_KEY  = os.environ['SUPABASE_RE_LLM_SERVICE_KEY']
ANTHROPIC_KEY = os.environ['ANTHROPIC_API_KEY']

MODEL                  = "claude-sonnet-4-6"
CONFIDENCE_THRESHOLD   = 0.75
CATEGORIZATION_VERSION = 1
CHUNK_CONTENT_MAX      = 4000
DOC_CONTEXT_MAX        = 3000
STATE_FILE             = Path("./batch_state.json")

ASSET_CLASSES = ["residential","office","retail","industrial","hospitality",
                 "healthcare","mixed_use","specialty","agricultural","land","virtual"]
TOPICS = ["legal","zoning","valuation","transactions","rental","investment",
          "financing","taxation","construction","operations","sustainability",
          "ownership","marketing","technology"]
CHUNK_TYPES = ["definition","rule","case_law","data_point","formula","procedure",
               "template","example","qa_pair","commentary","warning","metadata"]

SYSTEM_PROMPT = """You are a real estate knowledge classifier. Classify a piece of text from a real estate knowledge base across three orthogonal axes.

# CRITICAL: AXIS DISCIPLINE
The three axes are NOT interchangeable. Each value belongs to exactly ONE axis.
Before returning, validate every value against ONLY its axis's enum:
  - asset_classes[]  → ONLY values from AXIS 1 below
  - topics[]         → ONLY values from AXIS 2 below
  - chunk_type       → ONLY a value from AXIS 3 below
Common error to avoid: putting "land" or "residential" in topics; putting
"marketing" or "construction" in chunk_type or asset_classes; putting
"metadata" or "rule" in topics. Never do this.

# AXIS 1 — ASSET CLASSES (physical property types ONLY, multi-label)
Allowed: residential, office, retail, industrial, hospitality, healthcare,
mixed_use, specialty, agricultural, land, virtual.
- residential: single-family, multifamily, apartments, PPE, social/senior/student housing
- office: corporate, coworking, medical office, professional services
- retail: shops, malls, F&B, standalone restaurants
- industrial: warehouses, logistics, manufacturing, distribution, cold storage, flex
- hospitality: hotels, B&B, short-term rentals, serviced apartments
- healthcare: hospitals, clinics, EMS, senior care, life sciences buildings
- mixed_use: combinations not dominated by one class
- specialty: data centers, self-storage, entertainment, religious, civic, cemeteries, prisons, military, sports
- agricultural: farms, vineyards, forests, agricultural land
- land: undeveloped plots, building rights without buildings
- virtual: metaverse, tokenized RWA, NFT-deeded digital assets

If the knowledge applies generically across all asset classes (e.g. general contract law, civil code preamble), return an empty array.

# AXIS 2 — TOPICS (knowledge domains ONLY, multi-label)
Allowed: legal, zoning, valuation, transactions, rental, investment,
financing, taxation, construction, operations, sustainability, ownership,
marketing, technology.

ALWAYS include at least one topic. Order by relevance.

# AXIS 3 — CHUNK TYPE (kind of statement ONLY, single-label)
Allowed: definition, rule, case_law, data_point, formula, procedure,
template, example, qa_pair, commentary, warning, metadata.
- definition: a term and its meaning
- rule: statute, regulation, normative requirement
- case_law: court decisions, jurisprudence
- data_point: a number, statistic, fact, benchmark value
- formula: a calculation or equation
- procedure: step-by-step how-to
- template: model contracts, sample clauses, forms
- example: concrete instance illustrating a rule or method
- qa_pair: explicit Q&A from FAQ-style sources
- commentary: analysis, opinion, interpretation
- warning: caveats, restrictions, exceptions, "attention!" notes
- metadata: titles, ToC, headers (skip-this-at-retrieval marker)

If unclear, prefer 'commentary' over forcing a more specific type.

# TAGS
1–5 short lowercase snake_case tags capturing distinctive concepts NOT already in topics
(e.g. 'lex_weber', 'ldtr', 'minergie', 'cap_rate', 'ius_zone5', 'taux_reference').

# CONFIDENCE
Honest 0–1 self-assessment. Use ≥ 0.85 ONLY when the content is unambiguous.
Use < 0.7 if the content is short, ambiguous, or could plausibly fit other categories.

# REASONING
1–2 sentences. Match the source language (FR for French content, EN for English, etc.).

Always output via the classify tool. Never return prose.
"""

CLASSIFY_TOOL = {
    "name": "classify",
    "description": "Classify a real estate knowledge item across the three taxonomy axes.",
    "input_schema": {
        "type": "object",
        "required": ["topics", "chunk_type", "confidence", "reasoning"],
        "properties": {
            "asset_classes": {"type": "array", "items": {"type": "string", "enum": ASSET_CLASSES}},
            "topics":        {"type": "array", "minItems": 1, "items": {"type": "string", "enum": TOPICS}},
            "chunk_type":    {"type": "string", "enum": CHUNK_TYPES},
            "tags":          {"type": "array", "maxItems": 5, "items": {"type": "string", "pattern": "^[a-z0-9_]+$"}},
            "confidence":    {"type": "number", "minimum": 0, "maximum": 1},
            "reasoning":     {"type": "string"}
        }
    }
}


def render_user_message(item):
    if item['kind'] == 'entry':
        return (
            f"Item type: knowledge entry\n"
            f"Title: {item.get('title') or '(no title)'}\n"
            f"Country: {item.get('country','?')}\n"
            f"Language: {item.get('language','?')}\n"
            f"Existing legacy category: {item.get('legacy_category','?')} / {item.get('legacy_subcategory','?')}\n"
            f"---\n"
            f"{(item.get('content') or '')[:CHUNK_CONTENT_MAX]}\n"
            f"---\n"
            f"Classify per the system instructions."
        )
    if item['kind'] == 'document':
        return (
            f"Item type: document (whole-document classification)\n"
            f"Title: {item.get('title') or '(no title)'}\n"
            f"Document type: {item.get('document_type','?')}\n"
            f"Language: {item.get('language','?')}\n"
            f"Country: {item.get('country','?')}\n"
            f"Existing legacy category: {item.get('legacy_category','?')}\n"
            f"---\n"
            f"DESCRIPTION:\n{(item.get('description') or '(none)')[:1000]}\n\n"
            f"FIRST-CHUNK EXCERPT:\n{(item.get('first_chunk_excerpt') or '(none)')[:DOC_CONTEXT_MAX]}\n"
            f"---\n"
            f"Classify the document as a whole. The chunks within will be classified individually later."
        )
    if item['kind'] == 'chunk':
        return (
            f"Item type: document chunk\n"
            f"Parent document: {item.get('doc_title','?')}\n"
            f"Section: {item.get('section_title') or '(none)'}\n"
            f"Language: {item.get('language','?')}\n"
            f"---\n"
            f"{(item.get('content') or '')[:CHUNK_CONTENT_MAX]}\n"
            f"---\n"
            f"Classify per the system instructions."
        )
    raise ValueError(f"unknown kind {item['kind']}")


def fetch_pending_items(sb):
    items = []

    for schema in ['knowledge_ch', 'knowledge_global', 'knowledge_ae']:
        try:
            r = sb.schema(schema).table('entries').select(
                'id,title,content,country,language,legacy_category,legacy_subcategory'
            ).eq('categorization_status', 'pending').execute()
            for row in r.data:
                items.append({**row, 'kind': 'entry', 'schema': schema})
        except Exception as e:
            print(f"warn: failed to query {schema}.entries: {e}", file=sys.stderr)

    docs = sb.schema('knowledge_ch').table('documents').select(
        'id,title,description,document_type,language,country,legacy_category'
    ).eq('categorization_status', 'pending').execute()
    for d in docs.data:
        first_chunk = sb.schema('knowledge_ch').table('chunks').select('content').eq(
            'document_id', d['id']
        ).order('chunk_index').limit(1).execute()
        d['first_chunk_excerpt'] = first_chunk.data[0]['content'] if first_chunk.data else ''
        items.append({**d, 'kind': 'document', 'schema': 'knowledge_ch'})

    page_size = 1000
    offset = 0
    while True:
        ch = sb.schema('knowledge_ch').table('chunks').select(
            'id,document_id,section_title,content'
        ).eq('categorization_status', 'pending').range(offset, offset + page_size - 1).execute()
        if not ch.data:
            break
        doc_ids = list({c['document_id'] for c in ch.data if c.get('document_id')})
        docs_map = {}
        if doc_ids:
            r = sb.schema('knowledge_ch').table('documents').select(
                'id,title,language'
            ).in_('id', doc_ids).execute()
            docs_map = {d['id']: d for d in r.data}
        for c in ch.data:
            parent = docs_map.get(c.get('document_id'), {})
            items.append({
                **c, 'kind': 'chunk', 'schema': 'knowledge_ch',
                'doc_title': parent.get('title'), 'language': parent.get('language')
            })
        if len(ch.data) < page_size:
            break
        offset += page_size

    return items


def build_request(item):
    custom_id = f"{item['kind']}__{item['schema']}__{item['id']}"
    return {
        "custom_id": custom_id,
        "params": {
            "model": MODEL,
            "max_tokens": 1024,
            "system": SYSTEM_PROMPT,
            "tools": [CLASSIFY_TOOL],
            "tool_choice": {"type": "tool", "name": "classify"},
            "messages": [{"role": "user", "content": render_user_message(item)}],
        }
    }


def submit_batch(client, items):
    requests = [build_request(it) for it in items]
    print(f"Submitting batch with {len(requests)} requests...")
    batch = client.messages.batches.create(requests=requests)
    print(f"Batch ID: {batch.id}, status: {batch.processing_status}")
    return batch


def poll_batch(client, batch_id, sleep_seconds=60):
    while True:
        b = client.messages.batches.retrieve(batch_id)
        c = b.request_counts
        print(f"[{datetime.now():%H:%M:%S}] status={b.processing_status} "
              f"processing={c.processing} succeeded={c.succeeded} "
              f"errored={c.errored} canceled={c.canceled} expired={c.expired}",
              flush=True)
        if b.processing_status == 'ended':
            return b
        time.sleep(sleep_seconds)


def parse_custom_id(cid):
    kind, schema, item_id = cid.split('__', 2)
    return kind, schema, item_id


def parse_classification(result):
    if result.result.type != 'succeeded':
        return None, f"result type: {result.result.type}"
    msg = result.result.message
    for block in msg.content:
        if block.type == 'tool_use' and block.name == 'classify':
            return block.input, None
    return None, "no classify tool_use in response"


def write_back(sb, kind, schema, item_id, classification):
    asset_classes = classification.get('asset_classes') or None
    topics        = classification['topics']
    chunk_type    = classification.get('chunk_type')
    tags          = classification.get('tags') or None
    confidence    = classification['confidence']
    reasoning     = classification['reasoning']

    status = 'auto' if confidence >= CONFIDENCE_THRESHOLD else 'needs_review'

    update_payload = {
        'asset_classes': asset_classes,
        'topics': topics,
        'tags': tags,
        'categorization_confidence': confidence,
        'categorization_version': CATEGORIZATION_VERSION,
        'categorization_status': status,
    }
    if kind in ('entry', 'chunk'):
        update_payload['chunk_type'] = chunk_type

    table = {'entry': 'entries', 'document': 'documents', 'chunk': 'chunks'}[kind]
    sb.schema(schema).table(table).update(update_payload).eq('id', item_id).execute()

    if status == 'needs_review':
        col = 'content' if kind in ('entry', 'chunk') else 'description'
        excerpt_row = sb.schema(schema).table(table).select(col).eq('id', item_id).single().execute()
        excerpt = (excerpt_row.data or {}).get(col, '') or ''
        sb.schema('knowledge_global').table('categorization_review').insert({
            'source_schema': schema,
            'source_table': table,
            'source_id': item_id,
            'content_excerpt': excerpt[:1000],
            'suggested_asset_classes': asset_classes,
            'suggested_topics': topics,
            'suggested_chunk_type': chunk_type,
            'suggested_tags': tags,
            'confidence': confidence,
            'reasoning': reasoning,
        }).execute()


def ingest_results(client, sb, batch):
    success = 0
    review  = 0
    errors  = []
    for raw in client.messages.batches.results(batch.id):
        cid = raw.custom_id
        kind, schema, item_id = parse_custom_id(cid)
        classification, err = parse_classification(raw)
        if err:
            errors.append({'custom_id': cid, 'error': err})
            continue
        try:
            write_back(sb, kind, schema, item_id, classification)
            success += 1
            if classification['confidence'] < CONFIDENCE_THRESHOLD:
                review += 1
        except Exception as e:
            errors.append({'custom_id': cid, 'error': str(e)[:300]})

    print("\n=== Ingestion complete ===")
    print(f"Succeeded: {success}")
    print(f"Sent to review queue: {review}")
    print(f"Errors: {len(errors)}")
    if errors:
        Path('./errors.json').write_text(json.dumps(errors, indent=2))
        print("Error details written to ./errors.json")


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    state = load_state()

    if state.get('batch_id') and not state.get('ingested'):
        print(f"Resuming from saved batch_id: {state['batch_id']}")
        batch = poll_batch(client, state['batch_id'])
        ingest_results(client, sb, batch)
        state['ingested'] = True
        save_state(state)
        return

    items = fetch_pending_items(sb)
    print(f"Pending items: {len(items)}")
    if not items:
        print("Nothing to classify. Exiting.")
        return

    BATCH_LIMIT = 50_000
    if len(items) > BATCH_LIMIT:
        items = items[:BATCH_LIMIT]
        print(f"Limiting to first {BATCH_LIMIT}; re-run for the rest.")

    batch = submit_batch(client, items)
    state = {'batch_id': batch.id, 'submitted_at': datetime.now().isoformat(), 'item_count': len(items)}
    save_state(state)

    final = poll_batch(client, batch.id)
    ingest_results(client, sb, final)
    state['ingested'] = True
    save_state(state)


if __name__ == '__main__':
    main()

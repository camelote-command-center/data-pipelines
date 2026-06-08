"""
Batch-classify all pending knowledge items in re-LLM.
Idempotent. Re-run safely after interruption.
"""
import os, json, time, sys
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import anthropic
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(override=True)

SUPABASE_URL  = os.environ['SUPABASE_RE_LLM_URL']
SUPABASE_KEY  = os.environ['SUPABASE_RE_LLM_SERVICE_KEY']
ANTHROPIC_KEY = os.getenv('ANTHROPIC_API_KEY')  # required only for the anthropic provider

MODEL                  = "claude-sonnet-4-6"
CONFIDENCE_THRESHOLD   = 0.75
CATEGORIZATION_VERSION = 1
CHUNK_CONTENT_MAX      = 4000
DOC_CONTEXT_MAX        = 3000
STATE_FILE             = Path("./batch_state.json")

# Optional scope: when set, only classify pending documents+chunks whose
# document.source matches. Unset (default) = global backfill, the behaviour the
# daily cron relies on. Chunks are scoped via their parent document's source.
CLASSIFY_SOURCE        = os.getenv("CLASSIFY_SOURCE") or None

# Provider/transport. 'anthropic' (default) = Batches API, the path the daily
# cron uses — untouched. 'deepseek' = OpenAI-compatible synchronous
# chat/completions (no batch endpoint exists) run as a bounded-concurrency loop.
# The taxonomy logic, vocab fetch, scope filter and write_back are identical
# across providers — only the transport differs.
PROVIDER               = (os.getenv("CLASSIFY_PROVIDER") or "anthropic").lower()
DEEPSEEK_API_KEY       = os.getenv("DEEPSEEK_API_KEY")
DEEPSEEK_URL           = "https://api.deepseek.com/chat/completions"
# deepseek-v4-flash per the 2026-06 cutover. deepseek-chat / deepseek-reasoner
# retire 2026-07-24 — do not pin them. Env-overridable for future bumps.
DEEPSEEK_MODEL         = os.getenv("DEEPSEEK_MODEL", "deepseek-v4-flash")
DEEPSEEK_CONCURRENCY   = int(os.getenv("DEEPSEEK_CONCURRENCY", "8"))

# Run mode. 'backfill' (default) writes classifications. 'validate' is a no-write
# A/B: classify a sample with DeepSeek AND Sonnet, report agreement, write nothing.
CLASSIFY_MODE          = (os.getenv("CLASSIFY_MODE") or "backfill").lower()
VALIDATE_SAMPLE        = int(os.getenv("VALIDATE_SAMPLE", "80"))
# Validation ground truth: 'live' calls Sonnet per row; 'stored' compares against
# the production labels already in the DB (needed when the Anthropic key is capped).
VALIDATE_TRUTH         = (os.getenv("VALIDATE_TRUTH") or "live").lower()

# ---------------------------------------------------------------------------
# Taxonomy is DOMAIN-SCOPED and loaded from knowledge_global at runtime so this
# classifier stays in lock-step with the canonical vocab and the
# validate_taxonomy_* triggers. (The old hardcoded flat list used 'legal' as a
# topic; the canonical real_estate domain uses 're_law', and every topic must
# belong to the row's chosen domain or the UPDATE is rejected.)
# Populated by load_taxonomy(); SYSTEM_PROMPT / CLASSIFY_TOOL built after.
# ---------------------------------------------------------------------------
DOMAINS           = []   # ['real_estate', 'legal', ...]  (active)
DOMAIN_LABELS     = {}   # {domain: label_en}
DOMAIN_TOPICS     = {}   # {domain: [topic_key, ...]}
DOMAIN_ACS        = {}   # {domain: [asset_class_key, ...]}  (only real_estate today)
ALL_TOPICS        = []   # sorted union across domains (for the tool enum)
ALL_ASSET_CLASSES = []   # sorted union across domains
CHUNK_TYPES       = []   # chunk-type vocab keys

_FALLBACK_CHUNK_TYPES = ["definition","rule","case_law","data_point","formula",
                         "procedure","template","example","qa_pair","commentary",
                         "warning","metadata"]

SYSTEM_PROMPT = None     # built by build_system_prompt()
CLASSIFY_TOOL = None     # built by build_classify_tool()


def load_taxonomy(sb):
    """Load the active, domain-scoped taxonomy from knowledge_global."""
    global DOMAINS, DOMAIN_LABELS, DOMAIN_TOPICS, DOMAIN_ACS
    global ALL_TOPICS, ALL_ASSET_CLASSES, CHUNK_TYPES

    doms = sb.schema('knowledge_global').table('domains').select(
        'key,label_en').eq('is_active', True).execute().data
    DOMAINS = sorted(d['key'] for d in doms)
    DOMAIN_LABELS = {d['key']: (d.get('label_en') or d['key']) for d in doms}

    tops = sb.schema('knowledge_global').table('topics').select(
        'domain,key').eq('is_active', True).execute().data
    DOMAIN_TOPICS = {}
    for t in tops:
        DOMAIN_TOPICS.setdefault(t['domain'], []).append(t['key'])
    for d in DOMAIN_TOPICS:
        DOMAIN_TOPICS[d] = sorted(DOMAIN_TOPICS[d])

    acs = sb.schema('knowledge_global').table('asset_classes').select(
        'domain,key').eq('is_active', True).execute().data
    DOMAIN_ACS = {}
    for a in acs:
        DOMAIN_ACS.setdefault(a['domain'], []).append(a['key'])
    for d in DOMAIN_ACS:
        DOMAIN_ACS[d] = sorted(DOMAIN_ACS[d])

    ALL_TOPICS = sorted({t['key'] for t in tops})
    ALL_ASSET_CLASSES = sorted({a['key'] for a in acs})

    try:
        cts = sb.schema('knowledge_global').table('chunk_types').select(
            'key').eq('is_active', True).execute().data
        CHUNK_TYPES = sorted(c['key'] for c in cts) if cts else list(_FALLBACK_CHUNK_TYPES)
    except Exception as e:
        print(f"warn: chunk_types vocab load failed ({e}); using fallback", file=sys.stderr)
        CHUNK_TYPES = list(_FALLBACK_CHUNK_TYPES)

    if not DOMAINS or not ALL_TOPICS:
        raise RuntimeError("Taxonomy load returned empty domains/topics — aborting.")
    print(f"Taxonomy: {len(DOMAINS)} domains, {len(ALL_TOPICS)} topics, "
          f"{len(ALL_ASSET_CLASSES)} asset_classes, {len(CHUNK_TYPES)} chunk_types")


def build_classify_tool():
    return {
        "name": "classify",
        "description": "Classify a knowledge item: pick ONE domain, then topics and "
                       "asset_classes that belong to that domain.",
        "input_schema": {
            "type": "object",
            "required": ["domain", "topics", "chunk_type", "confidence", "reasoning"],
            "properties": {
                "domain":        {"type": "string", "enum": DOMAINS},
                "asset_classes": {"type": "array", "items": {"type": "string", "enum": ALL_ASSET_CLASSES}},
                "topics":        {"type": "array", "items": {"type": "string", "enum": ALL_TOPICS}},
                "chunk_type":    {"type": "string", "enum": CHUNK_TYPES},
                "tags":          {"type": "array", "maxItems": 5, "items": {"type": "string", "pattern": "^[a-z0-9_]+$"}},
                "confidence":    {"type": "number", "minimum": 0, "maximum": 1},
                "reasoning":     {"type": "string"}
            }
        }
    }


def build_system_prompt():
    lines = []
    for d in DOMAINS:
        tlist = DOMAIN_TOPICS.get(d)
        label = DOMAIN_LABELS.get(d, d)
        if tlist:
            lines.append(f"- {d} ({label}): topics = {', '.join(tlist)}")
        else:
            lines.append(f"- {d} ({label}): no sub-topics — return empty topics[]")
    domain_block = "\n".join(lines)
    ac_block = ", ".join(DOMAIN_ACS.get('real_estate', [])) or "(none)"
    chunk_block = ", ".join(CHUNK_TYPES)
    return f"""You are a knowledge classifier for a multi-domain knowledge base (Swiss/international real estate, law, finance, civic services and more). Classify one piece of text. Always output via the classify tool; never return prose.

# AXIS 1 — DOMAIN (pick EXACTLY ONE, required)
Choose the single domain the text most centrally belongs to. Allowed domains and the topics each one owns:
{domain_block}

# AXIS 2 — TOPICS (multi-label; MUST belong to the chosen domain)
Return only topic keys listed under your chosen domain above. Never borrow a topic from another domain. If the chosen domain shows "no sub-topics", return an empty array. Order by relevance.
Note: for a Geneva commune regulation (PLQ, PDCom, règlement de construction, zone, taxe, préemption, LCI/LDTR), the domain is almost always real_estate (topic re_law / zoning / construction), or legal when the text is a general statute.

# AXIS 3 — ASSET CLASSES (multi-label; ONLY when domain = real_estate)
Physical property types: {ac_block}.
- residential: housing, apartments, PPE, social/senior/student housing
- office, retail, industrial, hospitality, healthcare, mixed_use
- specialty: data centers, self-storage, civic, religious, cemeteries, sports, military
- agricultural, land, virtual
For any non-real_estate domain, return an empty array. If real-estate knowledge applies generically across all asset classes, return an empty array.

# AXIS 4 — CHUNK TYPE (single-label, required)
Allowed: {chunk_block}.
- definition; rule (statute, regulation, normative requirement); case_law; data_point (number/stat/benchmark); formula; procedure (step-by-step); template (model clauses/forms); example; qa_pair; commentary (analysis/opinion); warning (caveats, exceptions); metadata (titles, ToC, headers).
If unclear, prefer 'commentary'.

# TAGS
1–5 short lowercase snake_case tags capturing distinctive concepts NOT already in topics
(e.g. 'lex_weber', 'ldtr', 'minergie', 'plq', 'pdcom', 'taux_reference').

# CONFIDENCE
Honest 0–1 self-assessment. Use ≥ 0.85 ONLY when unambiguous; < 0.7 if short/ambiguous.

# REASONING
1–2 sentences. Match the source language (FR for French content, EN for English).
"""


def build_openai_tool():
    """Same classify schema as the Anthropic tool, in OpenAI function-calling
    shape (DeepSeek is OpenAI-compatible). Enums come from the canonical vocab."""
    return {
        "type": "function",
        "function": {
            "name": "classify",
            "description": "Classify a knowledge item: pick ONE domain, then topics and "
                           "asset_classes that belong to that domain.",
            "parameters": {
                "type": "object",
                "required": ["domain", "topics", "chunk_type", "confidence", "reasoning"],
                "properties": {
                    "domain":        {"type": "string", "enum": DOMAINS},
                    "asset_classes": {"type": "array", "items": {"type": "string", "enum": ALL_ASSET_CLASSES}},
                    "topics":        {"type": "array", "items": {"type": "string", "enum": ALL_TOPICS}},
                    "chunk_type":    {"type": "string", "enum": CHUNK_TYPES},
                    "tags":          {"type": "array", "maxItems": 5, "items": {"type": "string", "pattern": "^[a-z0-9_]+$"}},
                    "confidence":    {"type": "number", "minimum": 0, "maximum": 1},
                    "reasoning":     {"type": "string"},
                },
                "additionalProperties": False,
            },
        },
    }


# deepseek-v4-flash runs in thinking mode, which rejects forced tool_choice
# ("Thinking mode does not support this tool_choice"). So instead of function-
# calling we use response_format=json_object + this explicit output contract.
# The enums themselves live in SYSTEM_PROMPT; this only fixes the output shape.
DEEPSEEK_JSON_INSTRUCTION = (
    "\n\n---\n"
    "Respond with ONLY a single JSON object — no prose, no markdown fences — using "
    "exactly these keys:\n"
    '{"domain": "<one allowed domain key>", '
    '"topics": ["<topic keys belonging to the chosen domain>"], '
    '"asset_classes": ["<asset_class keys; [] unless domain is real_estate>"], '
    '"chunk_type": "<one allowed chunk_type key>", '
    '"tags": ["<0-5 lowercase_snake_case tags>"], '
    '"confidence": <number 0-1>, '
    '"reasoning": "<1-2 sentences>"}'
)


def classify_one_deepseek(item, max_retries=4):
    """Synchronous DeepSeek call for one item via JSON mode. Returns the parsed
    classification dict (raw — write_back applies domain-scoped filtering), or None."""
    payload = {
        "model": DEEPSEEK_MODEL,
        "max_tokens": 1024,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": render_user_message(item) + DEEPSEEK_JSON_INSTRUCTION},
        ],
        "response_format": {"type": "json_object"},
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    }
    backoff = 2.0
    for attempt in range(max_retries):
        try:
            r = requests.post(DEEPSEEK_URL, headers=headers, json=payload, timeout=120)
        except requests.RequestException:
            if attempt == max_retries - 1:
                return None
            time.sleep(backoff); backoff *= 2; continue
        if r.status_code == 429 or r.status_code >= 500:
            if attempt == max_retries - 1:
                return None
            time.sleep(backoff); backoff *= 2; continue
        if not r.ok:
            print(f"deepseek error {r.status_code}: {r.text[:200]}", file=sys.stderr)
            return None
        try:
            data = r.json()
            content = data["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            parsed.setdefault("confidence", 0.0)
            parsed.setdefault("reasoning", "")
            return parsed
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as e:
            print(f"deepseek parse error: {e}", file=sys.stderr)
            return None
    return None


def run_deepseek(sb, items, write=True):
    """Classify items via DeepSeek with bounded concurrency. When write=False,
    returns the classifications without persisting (used for the pre-run sample)."""
    results = []   # list of (item, classification|None)

    with ThreadPoolExecutor(max_workers=DEEPSEEK_CONCURRENCY) as ex:
        futs = {ex.submit(classify_one_deepseek, it): it for it in items}
        done = 0
        for fut in as_completed(futs):
            it = futs[fut]
            results.append((it, fut.result()))
            done += 1
            if done % 25 == 0:
                print(f"  classified {done}/{len(items)}", flush=True)

    if not write:
        return results

    # Persist sequentially (single supabase client; trigger disabled).
    success = review = errors = 0
    err_details = []
    for it, classification in results:
        if not classification:
            errors += 1; err_details.append({'id': it['id'], 'error': 'no classification'}); continue
        try:
            write_back(sb, it['kind'], it['schema'], it['id'], classification)
            success += 1
            if classification.get('confidence', 0) < CONFIDENCE_THRESHOLD:
                review += 1
        except Exception as e:
            errors += 1; err_details.append({'id': it['id'], 'error': str(e)[:300]})

    print("\n=== DeepSeek ingestion complete ===")
    print(f"Succeeded: {success}  (to review queue: {review})")
    print(f"Errors: {errors}")
    if err_details:
        Path('./errors.json').write_text(json.dumps(err_details, indent=2))
        print("Error details written to ./errors.json")
    return results


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
    source = CLASSIFY_SOURCE

    # entries: only in the global (unscoped) backfill — source scoping is
    # document/chunk oriented and the scoped sources have no entries.
    if not source:
        for schema in ['knowledge_ch', 'knowledge_global', 'knowledge_ae']:
            try:
                r = sb.schema(schema).table('entries').select(
                    'id,title,content,country,language,legacy_category,legacy_subcategory'
                ).eq('categorization_status', 'pending').execute()
                for row in r.data:
                    items.append({**row, 'kind': 'entry', 'schema': schema})
            except Exception as e:
                print(f"warn: failed to query {schema}.entries: {e}", file=sys.stderr)

    # documents
    dq = sb.schema('knowledge_ch').table('documents').select(
        'id,title,description,document_type,language,country,legacy_category'
    ).eq('categorization_status', 'pending')
    if source:
        dq = dq.eq('source', source)
    docs = dq.execute()
    for d in docs.data:
        first_chunk = sb.schema('knowledge_ch').table('chunks').select('content').eq(
            'document_id', d['id']
        ).order('chunk_index').limit(1).execute()
        d['first_chunk_excerpt'] = first_chunk.data[0]['content'] if first_chunk.data else ''
        items.append({**d, 'kind': 'document', 'schema': 'knowledge_ch'})

    # chunks: scoped via parent document's source using an inner embed.
    page_size = 1000
    offset = 0
    while True:
        cq = sb.schema('knowledge_ch').table('chunks')
        if source:
            cq = cq.select(
                'id,document_id,section_title,content,documents!inner(title,language,source)'
            ).eq('documents.source', source)
        else:
            cq = cq.select('id,document_id,section_title,content')
        ch = cq.eq('categorization_status', 'pending').range(offset, offset + page_size - 1).execute()
        if not ch.data:
            break

        if source:
            for c in ch.data:
                parent = c.get('documents') or {}
                items.append({
                    'id': c['id'], 'document_id': c.get('document_id'),
                    'section_title': c.get('section_title'), 'content': c.get('content'),
                    'kind': 'chunk', 'schema': 'knowledge_ch',
                    'doc_title': parent.get('title'), 'language': parent.get('language'),
                })
        else:
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
    domain        = classification.get('domain')
    # Enforce domain-scoped vocab so validate_taxonomy_* accepts the UPDATE:
    # drop any topic/asset_class that doesn't belong to the chosen domain.
    valid_topics  = [t for t in (classification.get('topics') or []) if t in DOMAIN_TOPICS.get(domain, [])]
    valid_acs     = [a for a in (classification.get('asset_classes') or []) if a in DOMAIN_ACS.get(domain, [])]
    topics        = valid_topics or None
    asset_classes = valid_acs or None
    chunk_type    = classification.get('chunk_type')
    tags          = classification.get('tags') or None
    confidence    = classification['confidence']
    reasoning     = classification['reasoning']

    status = 'auto' if confidence >= CONFIDENCE_THRESHOLD else 'needs_review'

    update_payload = {
        'domain': domain,
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


def _scoped_axes(cls):
    """Normalise a raw classification to the axes that would actually be stored
    (same domain-scoped topic/asset_class filtering as write_back), for A/B compare."""
    if not cls:
        return None
    domain = cls.get('domain')
    topics = sorted(t for t in (cls.get('topics') or []) if t in DOMAIN_TOPICS.get(domain, []))
    acs    = sorted(a for a in (cls.get('asset_classes') or []) if a in DOMAIN_ACS.get(domain, []))
    return {
        'domain': domain,
        'topics': topics,
        'asset_classes': acs,
        'chunk_type': cls.get('chunk_type'),
        'confidence': cls.get('confidence'),
    }


def classify_one_anthropic_sync(client, item, tool, max_retries=4):
    """Synchronous single-row Sonnet call (same model/prompt/tool as the batch
    path) used only as the validation ground truth. Returns parsed dict or None."""
    backoff = 2.0
    for attempt in range(max_retries):
        try:
            msg = client.messages.create(
                model=MODEL, max_tokens=1024, system=SYSTEM_PROMPT,
                tools=[tool], tool_choice={"type": "tool", "name": "classify"},
                messages=[{"role": "user", "content": render_user_message(item)}],
            )
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"anthropic error: {str(e)[:200]}", file=sys.stderr)
                return None
            time.sleep(backoff); backoff *= 2; continue
        for block in msg.content:
            if block.type == 'tool_use' and block.name == 'classify':
                return block.input
        return None
    return None


def fetch_classified_sample(sb, n):
    """Sample already-classified rows (categorization_version=2) WITH their stored
    axes, to A/B DeepSeek against production labels without calling Sonnet — used
    when the live Anthropic key is unavailable. Returns items renderable by
    render_user_message, each carrying its stored labels under '_stored'."""
    half = max(1, n // 2)
    out = []
    # documents carry no chunk_type column (that axis is entries/chunks only).
    docs = sb.schema('knowledge_ch').table('documents').select(
        'id,title,description,document_type,language,country,legacy_category,'
        'domain,topics,asset_classes'
    ).eq('categorization_version', 2).in_(
        'categorization_status', ['auto', 'needs_review']).limit(half).execute()
    for row in docs.data:
        fc = sb.schema('knowledge_ch').table('chunks').select('content').eq(
            'document_id', row['id']).order('chunk_index').limit(1).execute()
        row['first_chunk_excerpt'] = fc.data[0]['content'] if fc.data else ''
        out.append({**row, 'kind': 'document', 'schema': 'knowledge_ch',
                    '_stored': {k: row.get(k) for k in ('domain', 'topics', 'asset_classes')}})

    chunks = sb.schema('knowledge_ch').table('chunks').select(
        'id,document_id,section_title,content,domain,topics,asset_classes,chunk_type'
    ).eq('categorization_version', 2).in_(
        'categorization_status', ['auto', 'needs_review']).limit(n - len(out)).execute()
    doc_ids = list({c['document_id'] for c in chunks.data if c.get('document_id')})
    docs_map = {}
    if doc_ids:
        r = sb.schema('knowledge_ch').table('documents').select(
            'id,title,language').in_('id', doc_ids).execute()
        docs_map = {d['id']: d for d in r.data}
    for row in chunks.data:
        parent = docs_map.get(row.get('document_id'), {})
        out.append({**row, 'kind': 'chunk', 'schema': 'knowledge_ch',
                    'doc_title': parent.get('title'), 'language': parent.get('language'),
                    '_stored': {k: row.get(k) for k in ('domain', 'topics', 'asset_classes', 'chunk_type')}})
    return out


def run_validation(sb, client):
    """No-write A/B harness. Classifies a sample with DeepSeek and compares against
    a Sonnet ground truth — either live (VALIDATE_TRUTH=live, one Sonnet call per
    row) or the stored production labels (VALIDATE_TRUTH=stored, no Anthropic calls,
    used when the Anthropic key is capped). Writes nothing."""
    if VALIDATE_TRUTH == 'stored':
        sample = fetch_classified_sample(sb, VALIDATE_SAMPLE)
        truth_label = "STORED production labels"
    else:
        sample = fetch_pending_items(sb)[:VALIDATE_SAMPLE]
        truth_label = f"live Sonnet={MODEL}"
    if not sample:
        print("No rows available to validate. Exiting.")
        return
    print(f"Validation: {len(sample)} rows | DeepSeek={DEEPSEEK_MODEL} "
          f"vs {truth_label} | NO writes\n")

    # DeepSeek over the sample (concurrent, no write).
    ds_by_id = {it['id']: cls for it, cls in run_deepseek(sb, sample, write=False)}

    # Ground truth: stored labels, or a live Sonnet call per row.
    if VALIDATE_TRUTH == 'stored':
        truth_by_id = {it['id']: it['_stored'] for it in sample}
    else:
        an_tool = build_classify_tool()
        truth_by_id = {}
        with ThreadPoolExecutor(max_workers=DEEPSEEK_CONCURRENCY) as ex:
            futs = {ex.submit(classify_one_anthropic_sync, client, it, an_tool): it for it in sample}
            done = 0
            for fut in as_completed(futs):
                truth_by_id[futs[fut]['id']] = fut.result()
                done += 1
                if done % 25 == 0:
                    print(f"  sonnet {done}/{len(sample)}", flush=True)

    axes = ['domain', 'topics', 'asset_classes', 'chunk_type']
    matched = {a: 0 for a in axes}
    denom = {a: 0 for a in axes}   # per-axis denominator (chunk_type skips documents)
    full_match = comparable = ds_fail = truth_fail = 0
    disagreements = []

    for it in sample:
        ds = _scoped_axes(ds_by_id.get(it['id']))
        tr = _scoped_axes(truth_by_id.get(it['id']))
        if ds is None:
            ds_fail += 1
        if tr is None:
            truth_fail += 1
        if ds is None or tr is None:
            continue
        comparable += 1
        item_axes = ['domain', 'topics', 'asset_classes']
        if it['kind'] != 'document':
            item_axes.append('chunk_type')
        row_ok = True
        diffs = {}
        for a in item_axes:
            denom[a] += 1
            if ds[a] == tr[a]:
                matched[a] += 1
            else:
                row_ok = False
                diffs[a] = {'deepseek': ds[a], 'truth': tr[a]}
        if row_ok:
            full_match += 1
        else:
            disagreements.append({'id': it['id'], 'kind': it['kind'], 'diffs': diffs})

    print("\n=== Validation summary (no writes) ===")
    print(f"Ground truth: {truth_label}")
    print(f"Sample: {len(sample)} | comparable (both present): {comparable} | "
          f"DeepSeek failures: {ds_fail} | truth failures: {truth_fail}")
    if comparable:
        for a in axes:
            if denom[a]:
                print(f"  {a:14s} agreement: {matched[a]}/{denom[a]} = {100*matched[a]/denom[a]:.1f}%")
        print(f"  {'FULL ROW':14s} agreement: {full_match}/{comparable} = {100*full_match/comparable:.1f}%")
    if disagreements:
        Path('./validation_disagreements.json').write_text(json.dumps(disagreements, indent=2))
        print(f"\n{len(disagreements)} disagreements written to ./validation_disagreements.json")
        for d in disagreements[:15]:
            ax = ", ".join(f"{k}: ds={v['deepseek']} / truth={v['truth']}" for k, v in d['diffs'].items())
            print(f"  - {d['kind']} {d['id']}: {ax}")


def main():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load canonical domain-scoped taxonomy and build the (provider-agnostic) prompt.
    global SYSTEM_PROMPT, CLASSIFY_TOOL
    load_taxonomy(sb)
    SYSTEM_PROMPT = build_system_prompt()

    # ---- Validate mode: no-write DeepSeek-vs-Sonnet A/B (gated cutover check) ----
    if CLASSIFY_MODE == 'validate':
        if not DEEPSEEK_API_KEY:
            sys.exit("CLASSIFY_MODE=validate requires DEEPSEEK_API_KEY.")
        client = None
        if VALIDATE_TRUTH == 'live':
            if not ANTHROPIC_KEY:
                sys.exit("VALIDATE_TRUTH=live requires ANTHROPIC_API_KEY.")
            client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
        run_validation(sb, client)
        return

    # ---- DeepSeek provider: synchronous OpenAI-compatible loop -------------
    if PROVIDER == 'deepseek':
        if not DEEPSEEK_API_KEY:
            sys.exit("CLASSIFY_PROVIDER=deepseek but DEEPSEEK_API_KEY is not set.")
        items = fetch_pending_items(sb)
        print(f"Pending items (source={CLASSIFY_SOURCE or 'ALL'}, provider=deepseek): {len(items)}")
        if not items:
            print("Nothing to classify. Exiting.")
            return
        run_deepseek(sb, items, write=True)
        return

    # ---- Anthropic provider: Batches API (default; daily-cron path) --------
    if not ANTHROPIC_KEY:
        sys.exit("CLASSIFY_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set.")
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    CLASSIFY_TOOL = build_classify_tool()

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

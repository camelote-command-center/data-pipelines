/**
 * Reparse script — fixes old_owner_s, new_owner_s, and building_id/feuillet
 * for the 135 transactions inserted on 2026-03-09.
 *
 * Reads the `transaction` column (raw FAO text), sends to Claude to extract
 * owners, and fixes buildings JSON by splitting building_id PPE feuillet suffix.
 */

import Anthropic from '@anthropic-ai/sdk';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.SUPABASE_URL!;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseKey);

const anthropicApiKey = process.env.ANTHROPIC_API_KEY!;
if (!anthropicApiKey) {
  console.error('ERROR: ANTHROPIC_API_KEY is required');
  process.exit(1);
}
const anthropic = new Anthropic({ apiKey: anthropicApiKey });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------------------
// Claude extraction for owners only
// ---------------------------------------------------------------------------

const OWNER_PROMPT = `
Analyze this FAO transaction text and extract ONLY the old and new owners.
Return strictly JSON with this format:

{
  "old_owners": [
    {"name": "Owner name", "city": "City", "date": "DD.MM.YYYY or null"}
  ],
  "new_owners": [
    {"name": "Owner name", "city": "City", "date": null}
  ]
}

Rules:
- "Ancien(s)" are old owners, "Nouveau(x)" are new owners
- Extract name, city, and inscription date (after "inscrit dès le" or "inscrite dès le")
- If multiple owners, list each separately
- Remove titles like "M.", "Mme", "Mlle" from names
- If no date, use null
- Return only JSON, no explanations
`;

async function extractOwners(
  transactionText: string,
  retries = 0,
): Promise<{ old_owners: any[]; new_owners: any[] } | null> {
  try {
    const response = await anthropic.messages.create({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 2048,
      messages: [
        {
          role: 'user',
          content: `${OWNER_PROMPT}\n\nTransaction text:\n"${transactionText}"`,
        },
      ],
    });

    const text = response.content[0].type === 'text' ? response.content[0].text : '';
    const cleaned = text.replace(/^```json?\n?/m, '').replace(/\n?```$/m, '');
    return JSON.parse(cleaned);
  } catch (err) {
    if (retries >= 2) {
      console.error(`  Failed to extract owners: ${err}`);
      return null;
    }
    await sleep(2000);
    return extractOwners(transactionText, retries + 1);
  }
}

// ---------------------------------------------------------------------------
// Fix buildings JSON — split building_id PPE feuillet suffix
// ---------------------------------------------------------------------------

function fixBuildings(buildingsStr: string | null): string | null {
  if (!buildingsStr) return null;

  try {
    const buildings = JSON.parse(buildingsStr);
    if (!Array.isArray(buildings)) return buildingsStr;

    for (const b of buildings) {
      const bid = b.building_id || '';
      const match = bid.match(/^(\d+\/\d+)-(\d+)$/);
      if (match) {
        b.building_id = match[1];
        b.feuillet_number = match[2];
      }
    }

    return JSON.stringify(buildings);
  } catch {
    return buildingsStr;
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Reparse — Fix owners + buildings for recent transactions');
  console.log('='.repeat(60));

  // 1. Read the 135 records
  const { data: records, error } = await supabase
    .schema('bronze')
    .from('transactions')
    .select('affaire_number, transaction, buildings')
    .gt('created_at', '2026-03-09')
    .order('affaire_number');

  if (error) {
    console.error('DB read error:', error);
    process.exit(1);
  }

  console.log(`  Found ${records.length} records to reparse\n`);

  let updated = 0;
  let errors = 0;

  for (let i = 0; i < records.length; i++) {
    const rec = records[i];
    if (i % 10 === 0) console.log(`  Processing ${i + 1}/${records.length}...`);

    // Extract owners via Claude
    const owners = await extractOwners(rec.transaction);

    // Fix buildings
    const fixedBuildings = fixBuildings(rec.buildings);

    // Build update payload
    const update: Record<string, any> = {
      buildings: fixedBuildings,
    };

    if (owners) {
      update.old_owner_s = owners.old_owners?.length ? owners.old_owners : null;
      update.new_owner_s = owners.new_owners?.length ? owners.new_owners : null;
    }

    // Upsert
    const { error: upsertErr } = await supabase
      .schema('bronze')
      .from('transactions')
      .update(update)
      .eq('affaire_number', rec.affaire_number);

    if (upsertErr) {
      console.error(`  Error updating ${rec.affaire_number}: ${upsertErr.message}`);
      errors++;
    } else {
      updated++;
    }

    await sleep(500); // Rate limit Claude API
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  REPARSE COMPLETE`);
  console.log(`  Records processed: ${records.length}`);
  console.log(`  Updated:           ${updated}`);
  console.log(`  Errors:            ${errors}`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

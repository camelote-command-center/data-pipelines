/**
 * Forum Discussion Seeder
 *
 * One-time script. For each discussion in `discussions` that has 0 posts,
 * generates a first forum message with OpenAI and inserts it into `posts`.
 *
 * Environment variables:
 *   SUPABASE_URL              — xoxo project URL        (required)
 *   SUPABASE_SERVICE_ROLE_KEY — xoxo service_role key    (required)
 *   OPENAI_API_KEY            — OpenAI API key           (required)
 */

import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ADMIN_USER_ID = '8a20194c-c560-49c8-bfb7-1d3f727ceba6';
const OPENAI_MODEL = 'gpt-4o';
const DELAY_MS = 2_000;

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_KEY = process.env.OPENAI_API_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}
if (!OPENAI_KEY) {
  console.error('ERROR: OPENAI_API_KEY required');
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
const openai = new OpenAI({ apiKey: OPENAI_KEY });

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function postProcess(text: string): string {
  return text
    // Kill em dashes and en dashes
    .replace(/\u2014/g, ' - ')
    .replace(/\u2013/g, '-')
    // Remove spaces before Swiss punctuation
    .replace(/\s+([;:!?])/g, '$1')
    // Remove double spaces
    .replace(/  +/g, ' ')
    .trim();
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `Tu es un utilisateur enthousiaste du forum Helveti. Tu écris en français suisse décontracté, tu tutoies. Pas d'espace avant la ponctuation. Pas de tirets longs. Ton ton est naturel, curieux, et bienveillant - comme un vrai membre de communauté.`;

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Forum Discussion Seeder');
  console.log('='.repeat(60));

  // Fetch discussions with their topic titles
  const { data: discussions, error: dErr } = await supabase
    .from('discussions')
    .select('id, title, slug, topic_id, topics(title)')
    .order('id');

  if (dErr) {
    console.error('Discussion fetch error:', dErr.message);
    process.exit(1);
  }
  if (!discussions?.length) {
    console.log('  No discussions found. Done.');
    return;
  }

  // Filter to discussions with 0 posts
  const { data: postCounts } = await supabase
    .from('posts')
    .select('discussion_id');

  const discussionsWithPosts = new Set(
    (postCounts || []).map((p: any) => p.discussion_id),
  );

  const empty = discussions.filter((d: any) => !discussionsWithPosts.has(d.id));

  console.log(`  Discussions: ${discussions.length} total, ${empty.length} with 0 posts`);

  if (empty.length === 0) {
    console.log('  All discussions already have posts. Done.');
    return;
  }

  let seeded = 0;
  let failed = 0;

  for (const disc of empty) {
    const topicTitle = (disc as any).topics?.title || 'Général';
    console.log(`  [${topicTitle}] "${disc.title}"`);

    try {
      const completion = await openai.chat.completions.create({
        model: OPENAI_MODEL,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          {
            role: 'user',
            content: `Écris un premier message de forum pour lancer la discussion '${disc.title}' dans le sujet '${topicTitle}'. Le message doit être naturel, poser 2-3 questions ouvertes pour encourager les réponses, et faire 80-150 mots. Texte brut, pas de HTML.`,
          },
        ],
        temperature: 0.9,
        max_tokens: 500,
      });

      const raw = completion.choices[0]?.message?.content;
      if (!raw) {
        console.error(`    FAILED: empty response`);
        failed++;
        await sleep(DELAY_MS);
        continue;
      }

      const content = postProcess(raw);

      // Insert post
      const { error: insertErr } = await supabase.from('posts').insert({
        discussion_id: disc.id,
        content,
        created_by: ADMIN_USER_ID,
      });

      if (insertErr) {
        console.error(`    Insert error: ${insertErr.message}`);
        failed++;
      } else {
        const wc = content.split(/\s+/).filter(Boolean).length;
        console.log(`    OK: ${wc} words`);
        seeded++;
      }
    } catch (err) {
      console.error(`    OpenAI error: ${err}`);
      failed++;
    }

    await sleep(DELAY_MS);
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  DONE: ${seeded} seeded, ${failed} failed`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

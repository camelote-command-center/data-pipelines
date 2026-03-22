/**
 * Blog Article Generator
 *
 * Pulls pending topics from blog_content_queue, generates articles with OpenAI,
 * fetches cover images from Unsplash, and inserts drafts into blog_posts.
 *
 * Priority: safety > comparison > category_guide > city_guide > service > glossary
 *
 * Environment variables:
 *   SUPABASE_URL              — xoxo project URL        (required)
 *   SUPABASE_SERVICE_ROLE_KEY — xoxo service_role key    (required)
 *   OPENAI_API_KEY            — OpenAI API key           (required)
 *   UNSPLASH_ACCESS_KEY       — Unsplash API key         (optional)
 *   BATCH_SIZE                — articles per run         (default 5)
 */

import { createClient } from '@supabase/supabase-js';
import OpenAI from 'openai';

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const ADMIN_USER_ID = '8a20194c-c560-49c8-bfb7-1d3f727ceba6';
const OPENAI_MODEL = 'gpt-4o';
const DELAY_MS = 3_000;

// ---------------------------------------------------------------------------
// Env
// ---------------------------------------------------------------------------

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const OPENAI_KEY = process.env.OPENAI_API_KEY;
const UNSPLASH_KEY = process.env.UNSPLASH_ACCESS_KEY || null;
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '5', 10);

if (!SUPABASE_URL || !SUPABASE_KEY) {
  console.error('ERROR: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required');
  process.exit(1);
}
if (!OPENAI_KEY) {
  console.error('ERROR: OPENAI_API_KEY required');
  process.exit(1);
}
if (!UNSPLASH_KEY) {
  console.warn('WARNING: UNSPLASH_ACCESS_KEY not set - cover images will be skipped');
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

function slugify(text: string): string {
  return text
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 120);
}

function stripHtml(html: string): string {
  return html.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
}

function wordCount(html: string): number {
  return stripHtml(html).split(/\s+/).filter(Boolean).length;
}

// ---------------------------------------------------------------------------
// Banned / blocked
// ---------------------------------------------------------------------------

const BANNED_PHRASES = [
  "il est important de noter",
  "dans le monde de",
  "il convient de mentionner",
  "il convient de",
  "force est de constater",
  "en effet",
  "il va sans dire",
  "n'hésitez pas",
  "plongeons dans",
  "décryptons",
  "sans plus attendre",
  "voyage au coeur de",
  "dans cet article",
];

const COMPETITOR_DOMAINS = [
  'lolla.ch',
  'fgirl.ch',
  'petitesannonces.ch',
  'topannonces.ch',
  'anibis.ch',
];

// ---------------------------------------------------------------------------
// Competitor context for comparison articles
// ---------------------------------------------------------------------------

const COMPETITOR_CONTEXT = `Données concurrentes (pour contexte factuel):
- Lolla.ch: ~2'500 annonces, principalement Suisse romande, interface datée, pas de vérification de profil, gratuit pour les annonceurs
- FGirl.ch: ~3'000+ annonces, Suisse romande, interface basique, contenu en français/anglais, pas de modération visible
- Petitesannonces.ch: 114'000+ annonces érotiques (toutes catégories), Suisse romande, plateforme généraliste (pas spécialisée), interface chargée avec publicités
- TopAnnonces.ch: quelques centaines d'annonces escort, Suisse romande, plateforme généraliste, interface simple
- Helveti.ch: plateforme spécialisée, interface moderne, filtres par canton/ville/catégorie, profils vérifiés, photos modérées, multilingue (FR/DE/EN/IT), gratuit pour les annonceurs, forum communautaire, glossaire`;

// ---------------------------------------------------------------------------
// Post-processing
// ---------------------------------------------------------------------------

function postProcess(html: string): string {
  let result = html
    // Kill em dashes and en dashes
    .replace(/\u2014/g, ' - ')
    .replace(/\u2013/g, '-')
    // Remove spaces before Swiss punctuation
    .replace(/\s+([;:!?])/g, '$1')
    // Fix currency: "1'500 CHF" or "150 CHF" → "CHF 1'500.-" or "CHF 150.-"
    .replace(/([\d][\d']*(?:\.\d{2})?)\s*(?:CHF|francs?)/gi, (_, amount) => {
      return amount.includes('.') ? `CHF ${amount}` : `CHF ${amount}.-`;
    })
    // Remove double spaces
    .replace(/  +/g, ' ')
    .trim();

  // Strip links to competitor domains
  for (const domain of COMPETITOR_DOMAINS) {
    // Remove full <a> tags linking to competitors, keep the inner text
    const pattern = new RegExp(
      `<a\\s[^>]*href="[^"]*${domain.replace('.', '\\.')}[^"]*"[^>]*>(.*?)</a>`,
      'gi',
    );
    result = result.replace(pattern, '$1');
  }

  // Ensure all remaining external <a> tags have target="_blank" rel="noopener"
  result = result.replace(
    /<a\s+(href="https?:\/\/[^"]*")/gi,
    (match, hrefAttr) => {
      // Skip if already has target
      if (match.includes('target=')) return match;
      return `<a ${hrefAttr} target="_blank" rel="noopener"`;
    },
  );

  return result;
}

function validateContent(html: string, title: string): { ok: boolean; warnings: string[] } {
  const warnings: string[] = [];
  const wc = wordCount(html);

  if (wc < 500) warnings.push(`WORD_COUNT_LOW: ${wc} words (need 700+)`);
  if (wc > 1500) warnings.push(`WORD_COUNT_HIGH: ${wc} words`);

  // Check banned phrases
  const textLower = stripHtml(html).toLowerCase();
  const found = BANNED_PHRASES.filter((p) => textLower.includes(p));
  if (found.length > 2) warnings.push(`BANNED_PHRASES(${found.length}): ${found.join(', ')}`);

  // Check em/en dashes survived post-processing
  if (html.includes('\u2014') || html.includes('\u2013')) {
    warnings.push('DASHES: em/en dashes still present');
  }

  // Check space before punctuation (outside HTML tags)
  const textOnly = stripHtml(html);
  if (/\s[;:!?]/.test(textOnly)) {
    warnings.push('PUNCTUATION: spaces before ;:!? in text');
  }

  // Check competitor links survived post-processing
  for (const domain of COMPETITOR_DOMAINS) {
    if (html.includes(`href="`) && html.includes(domain)) {
      warnings.push(`COMPETITOR_LINK: link to ${domain} detected`);
    }
  }

  // Check external links exist (at least 1)
  const extLinkCount = (html.match(/<a\s[^>]*href="https?:\/\//gi) || []).length;
  if (extLinkCount === 0) {
    warnings.push('NO_EXTERNAL_LINKS: 0 external links found');
  }

  // Check all external links have target="_blank"
  const extLinksWithoutTarget = (
    html.match(/<a\s+href="https?:\/\/[^"]*"(?![^>]*target=)[^>]*>/gi) || []
  ).length;
  if (extLinksWithoutTarget > 0) {
    warnings.push(`LINKS_NO_TARGET: ${extLinksWithoutTarget} external links missing target="_blank"`);
  }

  return { ok: wc >= 500, warnings };
}

// ---------------------------------------------------------------------------
// System prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `Tu es rédacteur pour Helveti, une plateforme suisse d'annonces de services personnels. Tu écris en français suisse.

Ton style:
- Amical et un peu cheeky, comme un pote qui connait bien le milieu
- Tu tutoies le lecteur
- Phrases courtes et percutantes. Pas de blabla corporate
- Humour léger bienvenu, jamais vulgaire
- Tu donnes des conseils concrets et pratiques, pas des généralités
- Tu fais référence à la Suisse (villes, cantons, CHF, coutumes locales)

Format:
- Écris en HTML: utilise <h2>, <h3>, <p>, <ul>, <li>, <strong>, <em>
- PAS de <h1> (le titre est rendu séparément)
- 4-6 sous-titres <h2>
- MINIMUM 700 mots, idéalement 800-1000. C'est très important: un article de moins de 650 mots est trop court. Développe chaque section avec des exemples concrets, des anecdotes, des prix en CHF.
- Termine par un paragraphe qui invite naturellement à parcourir les annonces sur Helveti

Liens externes:
- Inclus 2-3 liens externes pertinents par article, en HTML: <a href="URL" target="_blank" rel="noopener">texte</a>
- Sources fiables: articles de presse suisse (RTS, Le Temps, 20 Minutes, SWI swissinfo.ch), sites officiels (admin.ch, ch.ch), Wikipédia francophone, sites de santé (bag.admin.ch)
- Pour les articles sur la sécurité: liens vers les associations (Aspasie Genève https://aspasie.ch, Fleur de Pavé Lausanne https://fleursdepave.ch, Procore https://procore-info.ch)
- Pour les guides par canton: liens vers les sites officiels du canton ou de la ville
- Pour les articles sur les services: liens vers Wikipedia pour les définitions générales
- JAMAIS de liens vers des sites concurrents (lolla.ch, fgirl.ch, petitesannonces.ch, topannonces.ch, anibis.ch)
- Les liens doivent être intégrés naturellement dans le texte, pas listés à la fin

Règles strictes:
- JAMAIS d'espace avant les signes de ponctuation (: ; ! ?) - c'est du français suisse
- JAMAIS de tirets longs (\u2014 ou \u2013), utilise des tirets simples (-) ou reformule
- Prix en format suisse: CHF avant le montant, apostrophe pour les milliers, point-tiret pour les montants ronds: CHF 1'500.- ou CHF 149.50. JAMAIS "1'500 CHF" ou "1500 francs"
- JAMAIS ces expressions: "il est important de noter", "dans le monde de", "il convient de", "force est de constater", "en effet", "il va sans dire", "n'hésitez pas", "plongeons dans", "décryptons", "sans plus attendre", "voyage au coeur de"
- Commence par quelque chose d'accrocheur: une question, une affirmation audacieuse, un mini-scénario. JAMAIS par "Dans cet article..."
- Varie la longueur des phrases. Certaines très courtes. D'autres plus longues.`;

// ---------------------------------------------------------------------------
// Topic-specific prompts
// ---------------------------------------------------------------------------

interface QueueRow {
  id: number;
  topic_type: string;
  topic_key: string;
  topic_title: string;
  target_keyword: string | null;
  lang: string;
}

async function buildUserPrompt(row: QueueRow): Promise<string> {
  const kw = row.target_keyword || row.topic_title;

  switch (row.topic_type) {
    case 'glossary': {
      const { data } = await supabase
        .from('glossary_terms')
        .select('term, definition')
        .eq('slug', row.topic_key)
        .eq('lang', 'fr')
        .single();
      const term = data?.term || row.topic_title;
      const def = data?.definition || '';
      return `Écris un article fun et informatif qui explique le terme '${term}' (${def}). Mot-clé cible: '${kw}'. Couvre la définition, comment ça se passe en pratique, les attentes des deux côtés, et un conseil pour les débutants. Rappel: minimum 700 mots.`;
    }

    case 'service': {
      const { data } = await supabase
        .from('services')
        .select('name')
        .eq('slug', row.topic_key)
        .single();
      const name = data?.name || row.topic_title;
      return `Écris un guide complet sur '${name}'. Mot-clé cible: '${kw}'. Explique ce que c'est concrètement, comment choisir un bon prestataire en Suisse, les tarifs habituels en CHF si pertinent, et les questions qu'on n'ose pas poser. Rappel: minimum 700 mots.`;
    }

    case 'city_guide': {
      const { data: canton } = await supabase
        .from('cantons')
        .select('id, name')
        .eq('slug', row.topic_key)
        .single();
      const cantonName = canton?.name || row.topic_title;
      let cityList = '';
      if (canton?.id) {
        const { data: cities } = await supabase
          .from('listings_ads')
          .select('cities!inner(name)')
          .eq('canton_id', canton.id)
          .eq('status', 'active')
          .limit(200);
        if (cities?.length) {
          const unique = [...new Set(cities.map((c: any) => c.cities?.name).filter(Boolean))];
          cityList = unique.slice(0, 10).join(', ');
        }
      }
      return `Écris un guide pratique des services personnels dans le canton de ${cantonName}. Mot-clé cible: '${kw}'. Villes principales: ${cityList || 'à découvrir'}. Couvre les quartiers/zones, le stationnement, les transports, la discrétion, et ce qui rend ${cantonName} spécial. Rappel: minimum 700 mots.`;
    }

    case 'category_guide': {
      const { data } = await supabase
        .from('categories')
        .select('name')
        .eq('slug', row.topic_key)
        .single();
      const name = data?.name || row.topic_title;
      return `Écris un guide sur la catégorie '${name}' sur Helveti. Mot-clé cible: '${kw}'. Explique ce que cette catégorie regroupe, comment parcourir les annonces efficacement, les filtres utiles, et des conseils pour une bonne première expérience. Rappel: minimum 700 mots.`;
    }

    case 'comparison': {
      const key = row.topic_key;

      // "helveti-vs-*" direct comparison
      if (key.startsWith('helveti-vs-')) {
        return `Écris un article comparatif: '${row.topic_title}'. Mot-clé cible: '${kw}'.

Structure en tableau comparatif HTML (<table>) avec ces critères:
- Nombre d'annonces (approximatif)
- Couverture géographique (cantons)
- Catégories disponibles
- Qualité des profils / photos
- Filtres de recherche
- Sécurité et vérification
- Interface / facilité d'utilisation
- Langue du site
- Prix pour les annonceurs
- Fonctionnalités uniques

Sois honnête mais positionne clairement Helveti comme le meilleur choix. Mentionne les vrais avantages des concurrents (crédibilité). Puis explique pourquoi Helveti fait mieux ou différemment.

Important: NE mets PAS de lien vers le site concurrent. Tu peux le nommer mais ne donne pas l'URL.

Intègre naturellement 2-3 liens internes vers Helveti:
- <a href="/">Parcourir les annonces sur Helveti</a>
- <a href="/category/girls">Voir les annonces Girls</a>
- Un lien vers une catégorie pertinente

${COMPETITOR_CONTEXT}

Rappel: minimum 700 mots.`;
      }

      // Roundup / best-of articles
      if (key.startsWith('meilleurs-sites-') || key.startsWith('alternatives-')) {
        return `Écris un article classement: '${row.topic_title}'. Mot-clé cible: '${kw}'.

Présente 4-5 plateformes avec avantages/inconvénients de chacune. Helveti doit apparaitre en première position avec la description la plus développée.

Pour chaque concurrent:
- 1-2 phrases sur ce qu'ils font bien
- 1-2 phrases sur leurs limites
- PAS de lien vers leur site

Pour Helveti:
- 3-4 phrases détaillées sur les avantages
- Liens internes: <a href="/">Parcourir les annonces</a> et <a href="/category/girls">Voir les annonces</a>

Termine par un tableau récapitulatif HTML (<table>) comparant les 5 plateformes sur 4-5 critères clés.

${COMPETITOR_CONTEXT}

Rappel: minimum 700 mots.`;
      }

      // "choisir-site-*" and "publier-annonce-*" guides
      return `Écris un guide pratique: '${row.topic_title}'. Mot-clé cible: '${kw}'.

Donne des critères objectifs pour choisir un bon site d'annonces. Explique comment évaluer:
- La taille de l'audience
- La sécurité et la modération
- La qualité de l'interface
- Le rapport qualité/prix
- La discrétion

Puis applique ces critères aux principales plateformes suisses (sans lien vers les concurrents). Conclusion naturelle: Helveti coche toutes les cases.

Intègre naturellement 2-3 liens internes vers Helveti:
- <a href="/">Parcourir les annonces sur Helveti</a>
- <a href="/category/girls">Voir les annonces Girls</a>

${COMPETITOR_CONTEXT}

Rappel: minimum 700 mots.`;
    }

    case 'safety':
    default:
      return `Écris un article pratique et rassurant sur: '${row.topic_title}'. Mot-clé cible: '${kw}'. Donne des conseils concrets, applicables en Suisse. Pas moralisateur, pas flippant, juste pragmatique et bienveillant. Rappel: minimum 700 mots, développe bien chaque section.`;
  }
}

// ---------------------------------------------------------------------------
// Unsplash cover image
// ---------------------------------------------------------------------------

function getImageQuery(row: QueueRow): string {
  switch (row.topic_type) {
    case 'glossary':
      return 'romantic couple';
    case 'service':
      return 'spa wellness';
    case 'city_guide': {
      // Use canton name for more relevant results
      const name = row.topic_key.replace(/-/g, ' ');
      return `${name} switzerland`;
    }
    case 'category_guide':
      return 'nightlife city';
    case 'comparison':
      return 'business comparison';
    case 'safety':
      return 'safety padlock';
    default:
      return 'switzerland';
  }
}

interface UnsplashResult {
  url: string | null;
  credit: string | null;
  photographer: string | null;
}

async function fetchCoverImage(row: QueueRow): Promise<UnsplashResult> {
  if (!UNSPLASH_KEY) return { url: null, credit: null, photographer: null };

  const imageQuery = getImageQuery(row);

  // Try primary query, fall back to simpler query
  for (const query of [imageQuery, 'switzerland landscape']) {
    try {
      const encoded = encodeURIComponent(query);
      const res = await fetch(
        `https://api.unsplash.com/photos/random?query=${encoded}&orientation=landscape&content_filter=high`,
        { headers: { Authorization: `Client-ID ${UNSPLASH_KEY}` } },
      );
      if (!res.ok) {
        console.warn(`  Unsplash HTTP ${res.status} for "${query}"`);
        continue; // try fallback
      }
      const photo = await res.json();
      if (photo.urls?.regular) {
        return {
          url: photo.urls.regular,
          credit: `Photo by ${photo.user?.name || 'Unknown'} on Unsplash`,
          photographer: photo.user?.name || null,
        };
      }
    } catch (err) {
      console.warn(`  Unsplash error for "${query}": ${err}`);
    }
  }

  return { url: null, credit: null, photographer: null };
}

// ---------------------------------------------------------------------------
// Generate article with OpenAI
// ---------------------------------------------------------------------------

async function generateArticle(row: QueueRow): Promise<string | null> {
  const userPrompt = await buildUserPrompt(row);

  try {
    const completion = await openai.chat.completions.create({
      model: OPENAI_MODEL,
      messages: [
        { role: 'system', content: SYSTEM_PROMPT },
        { role: 'user', content: userPrompt },
      ],
      temperature: 0.85,
      max_tokens: 4000,
    });

    return completion.choices[0]?.message?.content || null;
  } catch (err) {
    console.error(`  OpenAI error for "${row.topic_title}": ${err}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Main pipeline
// ---------------------------------------------------------------------------

async function main() {
  console.log('='.repeat(60));
  console.log('  Blog Article Generator');
  console.log(`  Batch size: ${BATCH_SIZE}`);
  console.log('='.repeat(60));

  // 1. Fetch pending queue items, prioritised
  const { data: queue, error: qErr } = await supabase
    .from('blog_content_queue')
    .select('*')
    .eq('status', 'pending')
    .eq('lang', 'fr')
    .order('id')
    .limit(200);

  if (qErr) {
    console.error('Queue fetch error:', qErr.message);
    process.exit(1);
  }
  if (!queue || queue.length === 0) {
    console.log('  No pending topics. Done.');
    return;
  }

  // Prioritise: safety > comparison > category_guide > city_guide > service > glossary
  const priority: Record<string, number> = {
    safety: 1,
    comparison: 2,
    category_guide: 3,
    city_guide: 4,
    service: 5,
    glossary: 6,
  };
  const sorted = queue.sort(
    (a: any, b: any) => (priority[a.topic_type] || 9) - (priority[b.topic_type] || 9),
  );
  const batch = sorted.slice(0, BATCH_SIZE);

  console.log(`  Queue: ${queue.length} pending, processing ${batch.length}`);
  console.log(`  Types: ${batch.map((r: any) => r.topic_type).join(', ')}`);

  let generated = 0;
  let failed = 0;

  for (const row of batch) {
    console.log(`\n  [${row.topic_type}] "${row.topic_title}"`);

    // Mark as generating
    await supabase
      .from('blog_content_queue')
      .update({ status: 'generating' })
      .eq('id', row.id);

    // Generate article
    const rawHtml = await generateArticle(row);
    if (!rawHtml) {
      console.error(`    FAILED: OpenAI returned null`);
      await supabase
        .from('blog_content_queue')
        .update({ status: 'pending' })
        .eq('id', row.id);
      failed++;
      await sleep(DELAY_MS);
      continue;
    }

    // Post-process
    let html = postProcess(rawHtml);

    // Validate
    let validation = validateContent(html, row.topic_title);
    if (validation.warnings.length) {
      console.warn(`    Warnings: ${validation.warnings.join('; ')}`);
    }

    // If word count too low, retry once with stronger instruction
    if (!validation.ok) {
      console.log(`    Retrying (word count too low: ${wordCount(html)})...`);
      await sleep(DELAY_MS);
      const retry = await generateArticle(row);
      if (retry) {
        html = postProcess(retry);
        validation = validateContent(html, row.topic_title);
        if (validation.warnings.length) {
          console.warn(`    Retry warnings: ${validation.warnings.join('; ')}`);
        }
      }
      // Proceed regardless after retry
    }

    // Fetch cover image
    const cover = await fetchCoverImage(row);

    // Build metadata
    const slug = slugify(row.topic_title);
    const textContent = stripHtml(html);
    const wc = wordCount(html);
    const excerpt = textContent.slice(0, 200);
    const metaDesc = textContent.slice(0, 155);
    const readingTime = Math.ceil(wc / 200);

    const tags: string[] = [row.topic_type];
    if (row.target_keyword) tags.push(row.target_keyword);
    if (cover.photographer) tags.push('unsplash', cover.photographer);

    // Insert blog post
    const { data: post, error: insertErr } = await supabase
      .from('blog_posts')
      .insert({
        slug,
        title: row.topic_title,
        body: textContent,
        content_html: html,
        excerpt,
        meta_description: metaDesc,
        target_keyword: row.target_keyword,
        seo_title: `${row.topic_title} | Blog Helveti`,
        seo_description: metaDesc,
        cover_image: cover.url,
        tags,
        reading_time_minutes: readingTime,
        status: 'draft',
        author_user_id: ADMIN_USER_ID,
      })
      .select('id')
      .single();

    if (insertErr) {
      console.error(`    Insert error: ${insertErr.message}`);
      await supabase
        .from('blog_content_queue')
        .update({ status: 'pending' })
        .eq('id', row.id);
      failed++;
      await sleep(DELAY_MS);
      continue;
    }

    // Link back to queue
    await supabase
      .from('blog_content_queue')
      .update({
        status: 'draft_created',
        blog_post_id: post!.id,
        generated_at: new Date().toISOString(),
      })
      .eq('id', row.id);

    const extLinks = (html.match(/<a\s[^>]*href="https?:\/\//gi) || []).length;
    console.log(`    OK: ${wc} words, ${extLinks} ext links, slug="${slug}", cover=${cover.url ? 'yes' : 'no'}`);
    generated++;

    await sleep(DELAY_MS);
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log(`  DONE: ${generated} generated, ${failed} failed`);
  console.log('='.repeat(60));
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

// [2026-07-20] Tinjob decommission: reads RE-LLM bronze_ch.tinjob_* (was jobs-ch public.*)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders });

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  const ANTHROPIC_KEY = Deno.env.get("ANTHROPIC_API_KEY");
  const UNSPLASH_KEY = Deno.env.get("UNSPLASH_ACCESS_KEY");

  if (!ANTHROPIC_KEY) return new Response(JSON.stringify({ error: "Missing ANTHROPIC_API_KEY" }), { status: 500, headers: corsHeaders });

  const body = await req.json().catch(() => ({}));
  const { topic: manualTopic, is_comparison, auto } = body;

  // --- Determine topic ---
  let topicText = manualTopic;
  let isCompetitor = is_comparison || false;
  let competitorName = body.competitor_name || null;
  let contentPillar = body.content_pillar || null;
  let primaryKeyword = body.primary_keyword || null;
  let queueItemId: string | null = null;

  if (!topicText && auto) {
    // Check blog_content_queue for due articles
    const { data: dueItems } = await supabase
      .schema("bronze_ch").from("tinjob_blog_content_queue")
      .select("*")
      .eq("status", "queued")
      .lte("scheduled_for", new Date().toISOString())
      .order("scheduled_for", { ascending: true })
      .limit(1);

    if (!dueItems || dueItems.length === 0) {
      return new Response(JSON.stringify({ skipped: true, reason: "No articles due today" }), {
        headers: { ...corsHeaders, "Content-Type": "application/json" },
      });
    }

    const item = dueItems[0];
    topicText = item.topic || item.title;
    isCompetitor = item.is_competitor_comparison || false;
    competitorName = item.competitor_name;
    contentPillar = item.content_pillar;
    primaryKeyword = item.primary_keyword;
    queueItemId = item.id;

    // Mark as in_progress
    await supabase.schema("bronze_ch").from("tinjob_blog_content_queue").update({ status: "in_progress" }).eq("id", queueItemId);
  }

  if (!topicText) {
    return new Response(JSON.stringify({ error: "No topic provided and no queued articles due" }), {
      status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  // --- Fetch existing articles for internal linking ---
  const { data: existingArticles } = await supabase
    .schema("bronze_ch").from("tinjob_blog_articles")
    .select("title, slug")
    .eq("is_published", true)
    .order("published_at", { ascending: false })
    .limit(10);

  const internalLinks = (existingArticles || []).map((a: any) => `- ${a.title}: https://tinjob.ch/blog/${a.slug}`).join("\n");

  // --- Build system prompt ---
  const systemPrompt = `Tu es un expert SEO et redacteur de contenu pour TinJob, une plateforme suisse de recherche d'emploi intelligente basee sur les competences. Redige des articles en francais suisse, avec un ton friendly et jeune, coherent avec le reste du site.

Regles absolues:
- JAMAIS de tiret cadratin, utilise des tirets normaux (-) ou des tirets demi-cadratins
- JAMAIS d'espace avant la ponctuation: ecris "exemple:" et non "exemple :"
- JAMAIS d'espace avant ; ? !
- Nombres: CHF 1'234.-, 100'000, 1'234.56
- Tutoie JAMAIS le lecteur, utilise "vous"
- Longueur: 900 a 1'200 mots
- Termine avec une conclusion et un CTA subtil vers https://tinjob.ch
- Mentionne TinJob naturellement 1-2 fois max, sans etre promotionnel
${primaryKeyword ? `- Mot-cle principal a placer dans le H1, le premier paragraphe, et 2-3 fois dans le corps: "${primaryKeyword}"` : ""}
${contentPillar ? `- Pilier de contenu: ${contentPillar}` : ""}
${internalLinks ? `\nArticles existants pour liens internes (inclure 1-2 liens pertinents):\n${internalLinks}` : ""}

IMPORTANT: Reponds UNIQUEMENT en HTML valide. PAS de Markdown. Utilise des balises HTML:
- <h1> pour le titre principal (un seul)
- <h2> pour les sous-titres (3 a 5)
- <p> pour les paragraphes
- <strong> pour le gras
- <ul><li> pour les listes a puces
- <ol><li> pour les listes numerotees
- <a href="..."> pour les liens
- <table><thead><tr><th> et <tbody><tr><td> pour les tableaux

FORMAT DE REPONSE (JSON strict, rien d'autre):
{
  "title": "Titre accrocheur optimise SEO (max 60 caracteres)",
  "meta_title": "Titre meta (max 60 chars, peut differer du H1)",
  "meta_description": "Description meta SEO (max 155 caracteres)",
  "excerpt": "Resume en 2 phrases pour la card du blog",
  "content": "Contenu complet en HTML avec h1, h2, p, strong, ul, ol, a, table",
  "tags": ["tag1", "tag2", "tag3"],
  "category": "emploi|conseils|marche|comparatif|reconversion|droit|salaire|secteurs|technologie",
  "primary_keyword": "mot-cle principal",
  "secondary_keywords": ["mot-cle2", "mot-cle3"],
  "focus_keyphrase": "expression cle focus",
  "reading_time_minutes": 5,
  "unsplash_query": "mot-cle en anglais pour image Unsplash",
  "internal_links": [{"anchor": "texte", "url": "/blog/slug"}],
  "external_links": [{"anchor": "texte", "url": "https://...", "source": "nom"}]
}`;

  let userPrompt = `Ecris un article de blog sur: ${topicText}`;
  if (isCompetitor && competitorName) {
    userPrompt += `\n\nCet article compare TinJob avec ${competitorName}. Sois honnete et objectif. Focus sur des differences reelles. Mets en avant l'approche unique de TinJob (matching par competences, interface Tinder-like, CV sur mesure ATS).\nCategorie: comparatif`;
  }

  console.log("Generating:", topicText.substring(0, 80));

  // --- Call Anthropic ---
  const anthropicRes = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: { "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json" },
    body: JSON.stringify({ model: "claude-sonnet-4-6", max_tokens: 4096, system: systemPrompt, messages: [{ role: "user", content: userPrompt }] }),
  });

  if (!anthropicRes.ok) {
    const errText = await anthropicRes.text();
    await supabase.schema("bronze_ch").from("tinjob_blog_generation_logs").insert({ topic: topicText, success: false, error_message: errText });
    if (queueItemId) await supabase.schema("bronze_ch").from("tinjob_blog_content_queue").update({ status: "failed", error_message: errText }).eq("id", queueItemId);
    return new Response(JSON.stringify({ error: "AI failed" }), { status: 500, headers: corsHeaders });
  }

  const anthropicData = await anthropicRes.json();
  const rawText = anthropicData.content[0]?.text || "";

  let article: any;
  try {
    const jsonMatch = rawText.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error("No JSON");
    article = JSON.parse(jsonMatch[0]);
  } catch (e) {
    await supabase.schema("bronze_ch").from("tinjob_blog_generation_logs").insert({ topic: topicText, success: false, error_message: `Parse error: ${rawText.substring(0, 300)}` });
    if (queueItemId) await supabase.schema("bronze_ch").from("tinjob_blog_content_queue").update({ status: "failed", error_message: "JSON parse error" }).eq("id", queueItemId);
    return new Response(JSON.stringify({ error: "Parse failed" }), { status: 500, headers: corsHeaders });
  }

  // --- Fetch Unsplash image ---
  let coverImage = { url: "", alt: "", credit: "", creditUrl: "" };
  if (UNSPLASH_KEY && article.unsplash_query) {
    try {
      const unsplashRes = await fetch(
        `https://api.unsplash.com/search/photos?query=${encodeURIComponent(article.unsplash_query)}&orientation=landscape&per_page=5`,
        { headers: { Authorization: `Client-ID ${UNSPLASH_KEY}` } }
      );
      const unsplashData = await unsplashRes.json();
      if (unsplashData.results?.length > 0) {
        const photo = unsplashData.results[Math.floor(Math.random() * Math.min(3, unsplashData.results.length))];
        coverImage = {
          url: `${photo.urls.raw}&w=1200&q=80`,
          alt: `${article.title} - TinJob`,
          credit: photo.user.name,
          creditUrl: `${photo.user.links.html}?utm_source=tinjob&utm_medium=referral`,
        };
        if (photo.links?.download_location) {
          fetch(`${photo.links.download_location}?client_id=${UNSPLASH_KEY}`).catch(() => {});
        }
      }
    } catch (e) { console.error("Unsplash error:", e); }
  }

  // --- Generate slug ---
  const slug = article.title.toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").substring(0, 80);

  // --- Swiss punctuation fixes ---
  let content = article.content;
  content = content.replace(/ +:/g, ":").replace(/ +;/g, ";").replace(/ +\?/g, "?").replace(/ +!/g, "!").replace(/\u2014/g, "\u2013");
  let title = article.title.replace(/ +:/g, ":").replace(/\u2014/g, "\u2013");

  // --- Word count ---
  const wordCount = content.replace(/<[^>]+>/g, " ").split(/\s+/).filter(Boolean).length;

  // --- Save to DB ---
  const { data: saved, error: saveError } = await supabase
    .schema("bronze_ch").from("tinjob_blog_articles")
    .insert({
      slug,
      title,
      meta_title: article.meta_title || title,
      meta_description: article.meta_description,
      excerpt: article.excerpt,
      content,
      category: isCompetitor ? "comparatif" : (article.category || "emploi"),
      tags: article.tags || [],
      cover_image_url: coverImage.url,
      cover_image_alt: coverImage.alt,
      cover_image_credit: coverImage.credit,
      cover_image_credit_url: coverImage.creditUrl,
      og_image_url: coverImage.url,
      reading_time_minutes: article.reading_time_minutes || 5,
      word_count: wordCount,
      language: "fr",
      is_published: true,
      status: "published",
      published_at: new Date().toISOString(),
      canonical_url: `https://tinjob.ch/blog/${slug}`,
      is_competitor_comparison: isCompetitor,
      competitor_name: competitorName,
      primary_keyword: article.primary_keyword || primaryKeyword,
      secondary_keywords: article.secondary_keywords,
      focus_keyphrase: article.focus_keyphrase,
      content_pillar: contentPillar || article.category,
      internal_links_json: article.internal_links || [],
      external_links_json: article.external_links || [],
      source_type: "ai_generated",
      domain: "tinjob.ch",
    })
    .select()
    .single();

  if (saveError) {
    await supabase.schema("bronze_ch").from("tinjob_blog_generation_logs").insert({ topic: topicText, success: false, error_message: JSON.stringify(saveError) });
    if (queueItemId) await supabase.schema("bronze_ch").from("tinjob_blog_content_queue").update({ status: "failed", error_message: saveError.message }).eq("id", queueItemId);
    return new Response(JSON.stringify({ error: "Save failed", details: saveError }), { status: 500, headers: corsHeaders });
  }

  // --- Log success ---
  await supabase.schema("bronze_ch").from("tinjob_blog_generation_logs").insert({ topic: topicText, success: true, article_id: saved.id });

  // --- Update queue item ---
  if (queueItemId) {
    await supabase.schema("bronze_ch").from("tinjob_blog_content_queue").update({
      status: "generated",
      generated_article_id: saved.id,
    }).eq("id", queueItemId);
  }

  return new Response(JSON.stringify({
    success: true,
    article: { id: saved.id, slug: saved.slug, title: saved.title, url: `https://tinjob.ch/blog/${saved.slug}` },
  }), { headers: { ...corsHeaders, "Content-Type": "application/json" } });
});

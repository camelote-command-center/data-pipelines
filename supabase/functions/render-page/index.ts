// [2026-07-20] Tinjob decommission: reads RE-LLM bronze_ch.tinjob_* (was jobs-ch public.*)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SITE_URL = "https://tinjob.ch";
const SITE_NAME = "TinJob";
const FAVICON_URL = `${SUPABASE_URL}/storage/v1/object/public/site-identity/TinJob_logo_only.png`;
const LOGO_URL = FAVICON_URL;
const OG_DEFAULT_IMAGE = "https://pub-bb2e103a32db4e198524a2e9ed8f35b4.r2.dev/b1b9daa3-9179-4aa2-8284-9d5cd778d1c4/id-preview-c75f96de--e8733d4e-ff5b-452b-ad12-5c82aaab065b.lovable.app-1774358430916.png";
const JS_BUNDLE = "/assets/index-B-JB7MDL.js";
const CSS_BUNDLE = "/assets/index-Xd5cdbIK.css";

// --- HTML Shell ---

function htmlShell(opts: {
  title: string;
  metaDescription: string;
  canonicalUrl: string;
  ogImage: string;
  ogType: string;
  lang: string;
  bodyContent: string;
  structuredData?: string;
  breadcrumbs?: { name: string; url: string }[];
}): string {
  const breadcrumbLD = opts.breadcrumbs
    ? JSON.stringify({
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        itemListElement: opts.breadcrumbs.map((b, i) => ({
          "@type": "ListItem",
          position: i + 1,
          name: b.name,
          item: b.url,
        })),
      })
    : "";

  return `<!DOCTYPE html>
<html lang="${opts.lang}">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escapeHtml(opts.title)}</title>
  <meta name="description" content="${escapeHtml(opts.metaDescription)}">
  <link rel="canonical" href="${opts.canonicalUrl}">
  <link rel="icon" href="${FAVICON_URL}" type="image/png">
  <meta property="og:title" content="${escapeHtml(opts.title)}">
  <meta property="og:description" content="${escapeHtml(opts.metaDescription)}">
  <meta property="og:url" content="${opts.canonicalUrl}">
  <meta property="og:type" content="${opts.ogType}">
  <meta property="og:image" content="${opts.ogImage || OG_DEFAULT_IMAGE}">
  <meta property="og:site_name" content="${SITE_NAME}">
  <meta property="og:locale" content="fr_CH">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${escapeHtml(opts.title)}">
  <meta name="twitter:description" content="${escapeHtml(opts.metaDescription)}">
  <meta name="twitter:image" content="${opts.ogImage || OG_DEFAULT_IMAGE}">
  ${opts.structuredData ? `<script type="application/ld+json">${opts.structuredData}</script>` : ""}
  ${breadcrumbLD ? `<script type="application/ld+json">${breadcrumbLD}</script>` : ""}
  <link rel="stylesheet" crossorigin href="${CSS_BUNDLE}">
</head>
<body>
  ${opts.bodyContent}
  <div id="root"></div>
  <script type="module" crossorigin src="${JS_BUNDLE}"></script>
</body>
</html>`;
}

function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// --- Blog Article ---

async function renderBlogArticle(slug: string): Promise<Response | null> {
  const { data: article } = await supabase
    .schema("bronze_ch").from("tinjob_blog_articles")
    .select("*")
    .eq("slug", slug)
    .eq("is_published", true)
    .single();

  if (!article) return null;

  const structuredData = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Article",
    headline: article.title,
    description: article.meta_description || article.excerpt,
    image: article.og_image_url || article.cover_image_url || OG_DEFAULT_IMAGE,
    author: { "@type": "Organization", name: article.author_name || SITE_NAME },
    publisher: {
      "@type": "Organization",
      name: SITE_NAME,
      logo: { "@type": "ImageObject", url: LOGO_URL },
    },
    datePublished: article.published_at,
    dateModified: article.updated_at || article.published_at,
    mainEntityOfPage: article.canonical_url || `${SITE_URL}/blog/${slug}`,
    wordCount: article.word_count,
    articleSection: article.category,
    keywords: (article.tags || []).join(", "),
    inLanguage: article.language || "fr",
  });

  const coverHtml = article.cover_image_url
    ? `<img src="${article.cover_image_url}" alt="${escapeHtml(article.cover_image_alt || article.title)}" width="1080" height="600" style="width:100%;height:auto;">${article.cover_image_credit ? `<p style="font-size:0.8em;color:#666;">Photo: <a href="${article.cover_image_credit_url}" target="_blank" rel="noopener noreferrer">${escapeHtml(article.cover_image_credit)}</a> / Unsplash</p>` : ""}`
    : "";

  const bodyContent = `
<article>
  <header>
    ${coverHtml}
    <h1>${escapeHtml(article.title)}</h1>
    <p><time datetime="${article.published_at}">${new Date(article.published_at).toLocaleDateString("fr-CH")}</time> · ${article.reading_time_minutes || 5} min de lecture</p>
    ${article.excerpt ? `<p><strong>${escapeHtml(article.excerpt)}</strong></p>` : ""}
  </header>
  <div>${article.content}</div>
</article>`;

  return new Response(
    htmlShell({
      title: article.meta_title || article.title,
      metaDescription: article.meta_description || article.excerpt || "",
      canonicalUrl: article.canonical_url || `${SITE_URL}/blog/${slug}`,
      ogImage: article.og_image_url || article.cover_image_url || OG_DEFAULT_IMAGE,
      ogType: "article",
      lang: article.language || "fr",
      bodyContent,
      structuredData,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "Blog", url: `${SITE_URL}/blog` },
        { name: article.title, url: `${SITE_URL}/blog/${slug}` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- Blog Index ---

async function renderBlogIndex(): Promise<Response> {
  const { data: articles } = await supabase
    .schema("bronze_ch").from("tinjob_blog_articles")
    .select("slug, title, excerpt, cover_image_url, cover_image_alt, published_at, category, reading_time_minutes, tags")
    .eq("is_published", true)
    .order("published_at", { ascending: false })
    .limit(50);

  const articleListHtml = (articles || [])
    .map(
      (a) => `
    <article>
      ${a.cover_image_url ? `<img src="${a.cover_image_url}" alt="${escapeHtml(a.cover_image_alt || a.title)}" width="400" height="225" style="width:100%;max-width:400px;height:auto;">` : ""}
      <h2><a href="${SITE_URL}/blog/${a.slug}">${escapeHtml(a.title)}</a></h2>
      <p><time datetime="${a.published_at}">${new Date(a.published_at).toLocaleDateString("fr-CH")}</time> · ${a.reading_time_minutes || 5} min · ${escapeHtml(a.category || "")}</p>
      ${a.excerpt ? `<p>${escapeHtml(a.excerpt)}</p>` : ""}
    </article>`
    )
    .join("\n<hr>\n");

  const bodyContent = `
<main>
  <h1>Blog ${SITE_NAME} — Conseils emploi et carriere en Suisse</h1>
  <p>Guides, comparatifs et conseils pour optimiser votre recherche d'emploi en Suisse.</p>
  ${articleListHtml || "<p>Aucun article publie pour le moment.</p>"}
</main>`;

  return new Response(
    htmlShell({
      title: `Blog - ${SITE_NAME} | Conseils emploi Suisse`,
      metaDescription: `Decouvrez les derniers articles de ${SITE_NAME}: comparatifs de plateformes emploi, conseils CV, matching par competences et recherche d'emploi en Suisse.`,
      canonicalUrl: `${SITE_URL}/blog`,
      ogImage: OG_DEFAULT_IMAGE,
      ogType: "website",
      lang: "fr",
      bodyContent,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "Blog", url: `${SITE_URL}/blog` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- FAQ Index ---

async function renderFaq(): Promise<Response> {
  const { data: items } = await supabase
    .schema("bronze_ch").from("tinjob_faq_items")
    .select("question, slug, short_answer, full_answer, category")
    .eq("is_published", true)
    .order("sort_order", { ascending: true });

  const faqLD = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: (items || []).map((item) => ({
      "@type": "Question",
      name: item.question,
      acceptedAnswer: {
        "@type": "Answer",
        text: item.short_answer || item.full_answer || "",
      },
    })),
  });

  const faqHtml = (items || [])
    .map(
      (item) => `
    <section id="${item.slug}">
      <h2>${escapeHtml(item.question)}</h2>
      <div>${item.full_answer || item.short_answer || ""}</div>
    </section>`
    )
    .join("\n");

  const bodyContent = `
<main>
  <h1>Questions frequentes — ${SITE_NAME}</h1>
  <p>Tout ce que vous devez savoir sur ${SITE_NAME}, la plateforme suisse de matching emploi par competences.</p>
  ${faqHtml || "<p>Aucune question publiee pour le moment.</p>"}
</main>`;

  return new Response(
    htmlShell({
      title: `FAQ - ${SITE_NAME} | Questions frequentes`,
      metaDescription: `Trouvez les reponses a vos questions sur ${SITE_NAME}: fonctionnement, tarifs, matching par competences, generation de CV et recherche d'emploi en Suisse.`,
      canonicalUrl: `${SITE_URL}/faq`,
      ogImage: OG_DEFAULT_IMAGE,
      ogType: "website",
      lang: "fr",
      bodyContent,
      structuredData: faqLD,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "FAQ", url: `${SITE_URL}/faq` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- FAQ Single ---

async function renderFaqItem(slug: string): Promise<Response | null> {
  const { data: item } = await supabase
    .schema("bronze_ch").from("tinjob_faq_items")
    .select("*")
    .eq("slug", slug)
    .eq("is_published", true)
    .single();

  if (!item) return null;

  const faqLD = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "FAQPage",
    mainEntity: [
      {
        "@type": "Question",
        name: item.question,
        acceptedAnswer: {
          "@type": "Answer",
          text: item.short_answer || item.full_answer || "",
        },
      },
    ],
  });

  const bodyContent = `
<main>
  <article>
    <h1>${escapeHtml(item.question)}</h1>
    <div>${item.full_answer || item.short_answer || ""}</div>
  </article>
</main>`;

  return new Response(
    htmlShell({
      title: `${item.question} - FAQ ${SITE_NAME}`,
      metaDescription: item.meta_description || item.short_answer || "",
      canonicalUrl: item.canonical_url || `${SITE_URL}/faq/${slug}`,
      ogImage: item.og_image_url || OG_DEFAULT_IMAGE,
      ogType: "article",
      lang: "fr",
      bodyContent,
      structuredData: faqLD,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "FAQ", url: `${SITE_URL}/faq` },
        { name: item.question, url: `${SITE_URL}/faq/${slug}` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- Glossary Index ---

async function renderGlossaryIndex(): Promise<Response> {
  const { data: terms } = await supabase
    .schema("bronze_ch").from("tinjob_glossary_terms")
    .select("term, slug, short_definition, letter, category")
    .eq("is_published", true)
    .order("term", { ascending: true });

  const grouped: Record<string, typeof terms> = {};
  for (const term of terms || []) {
    const letter = (term.letter || term.term[0] || "#").toUpperCase();
    if (!grouped[letter]) grouped[letter] = [];
    grouped[letter].push(term);
  }

  const glossaryHtml = Object.keys(grouped)
    .sort()
    .map(
      (letter) => `
    <section>
      <h2>${letter}</h2>
      <dl>
        ${(grouped[letter] || [])
          .map(
            (t) => `
          <dt><a href="${SITE_URL}/glossaire/${t.slug}">${escapeHtml(t.term)}</a></dt>
          <dd>${escapeHtml(t.short_definition || "")}</dd>`
          )
          .join("")}
      </dl>
    </section>`
    )
    .join("\n");

  const definedTermsLD = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "DefinedTermSet",
    name: `Glossaire ${SITE_NAME}`,
    hasDefinedTerm: (terms || []).map((t) => ({
      "@type": "DefinedTerm",
      name: t.term,
      description: t.short_definition || "",
      url: `${SITE_URL}/glossaire/${t.slug}`,
    })),
  });

  const bodyContent = `
<main>
  <h1>Glossaire emploi — ${SITE_NAME}</h1>
  <p>Definitions des termes cles du marche de l'emploi, des RH et de la recherche de travail en Suisse.</p>
  ${glossaryHtml || "<p>Aucun terme publie pour le moment.</p>"}
</main>`;

  return new Response(
    htmlShell({
      title: `Glossaire emploi - ${SITE_NAME} | Definitions RH Suisse`,
      metaDescription: `Glossaire complet des termes du marche de l'emploi suisse: CV, ATS, matching, competences, entretien et plus. Par ${SITE_NAME}.`,
      canonicalUrl: `${SITE_URL}/glossaire`,
      ogImage: OG_DEFAULT_IMAGE,
      ogType: "website",
      lang: "fr",
      bodyContent,
      structuredData: definedTermsLD,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "Glossaire", url: `${SITE_URL}/glossaire` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- Glossary Term ---

async function renderGlossaryTerm(slug: string): Promise<Response | null> {
  const { data: term } = await supabase
    .schema("bronze_ch").from("tinjob_glossary_terms")
    .select("*")
    .eq("slug", slug)
    .eq("is_published", true)
    .single();

  if (!term) return null;

  const termLD = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "DefinedTerm",
    name: term.term,
    description: term.short_definition || term.full_definition || "",
    url: term.canonical_url || `${SITE_URL}/glossaire/${slug}`,
    inDefinedTermSet: {
      "@type": "DefinedTermSet",
      name: `Glossaire ${SITE_NAME}`,
      url: `${SITE_URL}/glossaire`,
    },
  });

  const bodyContent = `
<main>
  <article>
    <h1>${escapeHtml(term.term)}</h1>
    ${term.short_definition ? `<p><strong>${escapeHtml(term.short_definition)}</strong></p>` : ""}
    <div>${term.full_definition || ""}</div>
  </article>
</main>`;

  return new Response(
    htmlShell({
      title: `${term.term} - Glossaire ${SITE_NAME}`,
      metaDescription: term.meta_description || term.short_definition || "",
      canonicalUrl: term.canonical_url || `${SITE_URL}/glossaire/${slug}`,
      ogImage: term.og_image_url || OG_DEFAULT_IMAGE,
      ogType: "article",
      lang: "fr",
      bodyContent,
      structuredData: termLD,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "Glossaire", url: `${SITE_URL}/glossaire` },
        { name: term.term, url: `${SITE_URL}/glossaire/${slug}` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

// --- Comparison Page ---

async function renderComparison(slug: string): Promise<Response | null> {
  const { data: comp } = await supabase
    .schema("bronze_ch").from("tinjob_comparison_pages")
    .select("*")
    .eq("competitor_slug", slug)
    .eq("is_published", true)
    .single();

  if (!comp) return null;

  const comparisonTable = comp.comparison_table_json
    ? renderComparisonTable(comp.comparison_table_json)
    : "";

  const faqSection = comp.faq_items_json
    ? renderComparisonFaq(comp.faq_items_json)
    : "";

  const faqLD = comp.faq_items_json
    ? JSON.stringify({
        "@context": "https://schema.org",
        "@type": "FAQPage",
        mainEntity: (
          Array.isArray(comp.faq_items_json) ? comp.faq_items_json : []
        ).map((f: { question: string; answer: string }) => ({
          "@type": "Question",
          name: f.question,
          acceptedAnswer: { "@type": "Answer", text: f.answer },
        })),
      })
    : "";

  const bodyContent = `
<main>
  <article>
    <h1>${escapeHtml(comp.headline || `${SITE_NAME} vs ${comp.competitor_name}`)}</h1>
    ${comp.intro ? `<div>${comp.intro}</div>` : ""}
    ${comparisonTable}
    ${comp.full_content ? `<div>${comp.full_content}</div>` : ""}
    ${comp.summary_verdict ? `<section><h2>Verdict</h2><p>${escapeHtml(comp.summary_verdict)}</p></section>` : ""}
    ${faqSection}
  </article>
</main>`;

  return new Response(
    htmlShell({
      title: comp.meta_title || `${SITE_NAME} vs ${comp.competitor_name}`,
      metaDescription: comp.meta_description || comp.intro || "",
      canonicalUrl: comp.canonical_url || `${SITE_URL}/vs/${slug}`,
      ogImage: comp.og_image_url || OG_DEFAULT_IMAGE,
      ogType: "article",
      lang: "fr",
      bodyContent,
      structuredData: faqLD,
      breadcrumbs: [
        { name: "Accueil", url: SITE_URL },
        { name: "Comparatifs", url: `${SITE_URL}/vs` },
        { name: `${SITE_NAME} vs ${comp.competitor_name}`, url: `${SITE_URL}/vs/${slug}` },
      ],
    }),
    { headers: { "Content-Type": "text/html; charset=utf-8" } }
  );
}

function renderComparisonTable(tableJson: unknown): string {
  if (!Array.isArray(tableJson) || tableJson.length === 0) return "";
  const rows = tableJson as { feature: string; tinjob: string; competitor: string }[];
  return `
<table>
  <thead><tr><th>Critere</th><th>${SITE_NAME}</th><th>Concurrent</th></tr></thead>
  <tbody>
    ${rows.map((r) => `<tr><td>${escapeHtml(r.feature || "")}</td><td>${escapeHtml(r.tinjob || "")}</td><td>${escapeHtml(r.competitor || "")}</td></tr>`).join("")}
  </tbody>
</table>`;
}

function renderComparisonFaq(faqJson: unknown): string {
  if (!Array.isArray(faqJson) || faqJson.length === 0) return "";
  const items = faqJson as { question: string; answer: string }[];
  return `
<section>
  <h2>Questions frequentes</h2>
  ${items.map((f) => `<h3>${escapeHtml(f.question)}</h3><p>${escapeHtml(f.answer)}</p>`).join("")}
</section>`;
}

// --- Router ---

Deno.serve(async (req) => {
  try {
    const url = new URL(req.url);
    let path = url.searchParams.get("path") || "";

    if (!path && req.method === "POST") {
      const body = await req.json().catch(() => ({}));
      path = body.path || "";
    }

    if (!path) {
      return new Response(
        JSON.stringify({ error: "Missing 'path' parameter" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    // Normalize: strip trailing slash, ensure leading slash
    path = "/" + path.replace(/^\/+/, "").replace(/\/+$/, "");

    let response: Response | null = null;

    if (path === "/blog") {
      response = await renderBlogIndex();
    } else if (path.startsWith("/blog/")) {
      const slug = path.replace("/blog/", "");
      response = await renderBlogArticle(slug);
    } else if (path === "/faq") {
      response = await renderFaq();
    } else if (path.startsWith("/faq/")) {
      const slug = path.replace("/faq/", "");
      response = await renderFaqItem(slug);
    } else if (path === "/glossaire") {
      response = await renderGlossaryIndex();
    } else if (path.startsWith("/glossaire/")) {
      const slug = path.replace("/glossaire/", "");
      response = await renderGlossaryTerm(slug);
    } else if (path.startsWith("/vs/")) {
      const slug = path.replace("/vs/", "");
      response = await renderComparison(slug);
    }

    if (response) return response;

    return new Response(
      JSON.stringify({ error: "Page not found", path }),
      { status: 404, headers: { "Content-Type": "application/json" } }
    );
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error("render-page error:", msg);
    return new Response(
      JSON.stringify({ error: msg }),
      { status: 500, headers: { "Content-Type": "application/json" } }
    );
  }
});

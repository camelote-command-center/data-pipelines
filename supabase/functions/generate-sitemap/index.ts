// [2026-07-20] Tinjob decommission: reads RE-LLM bronze_ch.tinjob_* (was jobs-ch public.*)
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const SUPABASE_URL = Deno.env.get("SUPABASE_URL")!;
const SUPABASE_SERVICE_ROLE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const SITE_URL = "https://tinjob.ch";

interface SitemapEntry {
  loc: string;
  lastmod: string;
  changefreq: string;
  priority: string;
}

Deno.serve(async (req) => {
  try {
    const url = new URL(req.url);
    const format = url.searchParams.get("format");

    const entries: SitemapEntry[] = [];
    const now = new Date().toISOString().split("T")[0];

    // Static pages
    const staticPages = [
      { path: "/", priority: "1.0", changefreq: "weekly" },
      { path: "/offres-emploi", priority: "0.9", changefreq: "daily" },
      { path: "/blog", priority: "0.9", changefreq: "daily" },
      { path: "/glossaire", priority: "0.8", changefreq: "weekly" },
      { path: "/faq", priority: "0.8", changefreq: "weekly" },
      { path: "/vs", priority: "0.7", changefreq: "monthly" },
      { path: "/comment-ca-marche", priority: "0.7", changefreq: "monthly" },
      { path: "/a-propos", priority: "0.7", changefreq: "monthly" },
      { path: "/contact", priority: "0.6", changefreq: "monthly" },
    ];

    for (const page of staticPages) {
      entries.push({
        loc: `${SITE_URL}${page.path}`,
        lastmod: now,
        changefreq: page.changefreq,
        priority: page.priority,
      });
    }

    // Blog articles
    const { data: articles } = await supabase
      .schema("bronze_ch").from("tinjob_blog_articles")
      .select("slug, published_at, updated_at")
      .eq("is_published", true)
      .order("published_at", { ascending: false });

    if (articles) {
      for (const article of articles) {
        entries.push({
          loc: `${SITE_URL}/blog/${article.slug}`,
          lastmod: (article.updated_at || article.published_at || now).split("T")[0],
          changefreq: "monthly",
          priority: "0.8",
        });
      }
    }

    // Glossary terms
    const { data: glossaryTerms } = await supabase
      .schema("bronze_ch").from("tinjob_glossary_terms")
      .select("slug, updated_at")
      .eq("is_published", true);

    if (glossaryTerms) {
      for (const term of glossaryTerms) {
        entries.push({
          loc: `${SITE_URL}/glossaire/${term.slug}`,
          lastmod: (term.updated_at || now).split("T")[0],
          changefreq: "monthly",
          priority: "0.6",
        });
      }
    }

    // FAQ items
    const { data: faqItems } = await supabase
      .schema("bronze_ch").from("tinjob_faq_items")
      .select("slug, updated_at")
      .eq("is_published", true);

    if (faqItems) {
      for (const faq of faqItems) {
        entries.push({
          loc: `${SITE_URL}/faq/${faq.slug}`,
          lastmod: (faq.updated_at || now).split("T")[0],
          changefreq: "monthly",
          priority: "0.6",
        });
      }
    }

    // Comparison pages
    const { data: comparisons } = await supabase
      .schema("bronze_ch").from("tinjob_comparison_pages")
      .select("competitor_slug, updated_at")
      .eq("is_published", true);

    if (comparisons) {
      for (const comp of comparisons) {
        entries.push({
          loc: `${SITE_URL}/vs/${comp.competitor_slug}`,
          lastmod: (comp.updated_at || now).split("T")[0],
          changefreq: "monthly",
          priority: "0.7",
        });
      }
    }

    // Debug JSON mode
    if (format === "json") {
      return new Response(
        JSON.stringify({ urlCount: entries.length, entries }),
        { headers: { "Content-Type": "application/json" } }
      );
    }

    // Build XML sitemap
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${entries
  .map(
    (e) => `  <url>
    <loc>${e.loc}</loc>
    <lastmod>${e.lastmod}</lastmod>
    <changefreq>${e.changefreq}</changefreq>
    <priority>${e.priority}</priority>
  </url>`
  )
  .join("\n")}
</urlset>`;

    return new Response(xml, {
      headers: {
        "Content-Type": "application/xml; charset=utf-8",
        "Cache-Control": "public, max-age=3600, s-maxage=3600",
      },
    });
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error("Sitemap error:", msg);
    return new Response(
      `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>`,
      { status: 500, headers: { "Content-Type": "application/xml" } }
    );
  }
});

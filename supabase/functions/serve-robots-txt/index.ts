import { serve } from "https://deno.land/std@0.168.0/http/server.ts";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

const ROBOTS_TXT = `User-agent: *
Allow: /
Disallow: /tableau-de-bord
Disallow: /mes-matchs
Disallow: /profil-pro
Disallow: /compte
Disallow: /carriere
Disallow: /admin
Disallow: /bienvenue
Disallow: /emplois

Sitemap: https://tinjob.ch/sitemap.xml
`;

const LLMS_TXT = `# TinJob — Plateforme suisse de recherche d'emploi par competences

## A propos
TinJob.ch est une plateforme suisse de recherche d'emploi qui matche automatiquement les offres avec vos competences grace a l'intelligence artificielle.

## Fonctionnalites
- Matching par competences avec IA (score 0-100)
- Interface Tinder-like (swipe pour postuler)
- Generation de CV optimise ATS en 30 secondes
- Generation de lettre de motivation
- Explorateur de carriere IA
- Offres de jobup.ch et jobs.ch

## Pages principales
- https://tinjob.ch/ — Page d'accueil
- https://tinjob.ch/offres-emploi — Offres d'emploi
- https://tinjob.ch/blog — Blog emploi Suisse
- https://tinjob.ch/faq — Questions frequentes
- https://tinjob.ch/glossaire — Glossaire emploi
- https://tinjob.ch/comment-ca-marche — Comment ca marche
- https://tinjob.ch/a-propos — A propos
- https://tinjob.ch/vs/jobup — TinJob vs jobup.ch
- https://tinjob.ch/vs/jobs-ch — TinJob vs jobs.ch
- https://tinjob.ch/vs/linkedin — TinJob vs LinkedIn

## Contact
- Site: https://tinjob.ch
- Email: contact@tinjob.ch
`;

serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  const url = new URL(req.url);
  const file = url.searchParams.get("file") || "robots.txt";

  if (file === "llms.txt") {
    return new Response(LLMS_TXT, {
      headers: {
        ...corsHeaders,
        "Content-Type": "text/plain; charset=utf-8",
        "Cache-Control": "public, max-age=86400",
      },
    });
  }

  return new Response(ROBOTS_TXT, {
    headers: {
      ...corsHeaders,
      "Content-Type": "text/plain; charset=utf-8",
      "Cache-Control": "public, max-age=86400",
    },
  });
});

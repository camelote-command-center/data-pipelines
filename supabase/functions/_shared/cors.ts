const PRODUCTION_ORIGINS = [
  'https://billionaireslist.com',
  'https://www.billionaireslist.com',
  'https://billion-pulse-starter.lovable.app',
];

const STAGING_ORIGINS = [
  ...PRODUCTION_ORIGINS,
  'https://id-preview--5185f625-e28b-4e3d-9ada-36aa3ffad57c.lovable.app',
];

const DEVELOPMENT_ORIGINS = [
  ...STAGING_ORIGINS,
  'http://localhost:5173',
  'http://localhost:3000',
  'http://localhost:8080',
  'http://127.0.0.1:5173',
  'http://127.0.0.1:3000',
  'http://127.0.0.1:8080',
];

function getAllowedOrigins(): string[] {
  const env = Deno.env.get('ENVIRONMENT') || 'development';
  switch (env) {
    case 'production': return PRODUCTION_ORIGINS;
    case 'staging': return STAGING_ORIGINS;
    default: return DEVELOPMENT_ORIGINS;
  }
}

export interface CorsConfig {
  restrictToOrigins?: boolean;
  allowedOrigins?: string[];
}

export function getCorsHeaders(
  req: Request,
  config: CorsConfig = { restrictToOrigins: false }
): Record<string, string> {
  const origin = req.headers.get('origin') || '';
  const allowedOrigins = config.allowedOrigins || getAllowedOrigins();

  if (!config.restrictToOrigins) {
    return {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    };
  }

  if (allowedOrigins.includes(origin)) {
    return {
      'Access-Control-Allow-Origin': origin,
      'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Credentials': 'true',
      'Vary': 'Origin',
    };
  }

  return {
    'Access-Control-Allow-Origin': 'null',
    'Access-Control-Allow-Headers': '',
    'Access-Control-Allow-Methods': '',
  };
}

export function handleCorsPreflightRequest(
  req: Request,
  restricted = false
): Response | null {
  if (req.method !== 'OPTIONS') return null;
  const headers = getCorsHeaders(req, { restrictToOrigins: restricted });
  return new Response(null, { status: 204, headers });
}

export function isOriginAllowed(req: Request): boolean {
  const origin = req.headers.get('origin') || '';
  const allowedOrigins = getAllowedOrigins();
  if (!origin) return true;
  return allowedOrigins.includes(origin);
}

export function createBlockedOriginResponse(corsHeaders: Record<string, string>): Response {
  return new Response(
    JSON.stringify({ error: 'Origin not allowed' }),
    { status: 403, headers: { ...corsHeaders, 'Content-Type': 'application/json' } }
  );
}

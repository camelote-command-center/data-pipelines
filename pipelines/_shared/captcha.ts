/**
 * 2Captcha image CAPTCHA solver.
 *
 * Env vars:
 *   TWO_CAPTCHA_API_KEY - API key for 2captcha.com
 */

import { sleep } from './supabase.js';

const IN_URL = 'https://2captcha.com/in.php';
const RES_URL = 'https://2captcha.com/res.php';
const POLL_INTERVAL_MS = 5_000;
const MAX_WAIT_MS = 120_000;

export async function solveCaptcha(imageBase64: string): Promise<string> {
  const apiKey = process.env.TWO_CAPTCHA_API_KEY;
  if (!apiKey) throw new Error('TWO_CAPTCHA_API_KEY is required');

  // Submit CAPTCHA
  const submitParams = new URLSearchParams({
    key: apiKey,
    method: 'base64',
    body: imageBase64,
    json: '1',
  });

  const submitRes = await fetch(IN_URL, {
    method: 'POST',
    body: submitParams,
  });
  const submitData = (await submitRes.json()) as { status: number; request: string };

  if (submitData.status !== 1) {
    throw new Error(`2Captcha submit failed: ${submitData.request}`);
  }

  const captchaId = submitData.request;
  console.log(`  CAPTCHA submitted (id: ${captchaId}), polling for solution...`);

  // Poll for result
  const deadline = Date.now() + MAX_WAIT_MS;

  while (Date.now() < deadline) {
    await sleep(POLL_INTERVAL_MS);

    const resUrl = `${RES_URL}?key=${apiKey}&action=get&id=${captchaId}&json=1`;
    const pollRes = await fetch(resUrl);
    const pollData = (await pollRes.json()) as { status: number; request: string };

    if (pollData.status === 1) {
      console.log(`  CAPTCHA solved`);
      return pollData.request;
    }

    if (pollData.request !== 'CAPCHA_NOT_READY') {
      throw new Error(`2Captcha error: ${pollData.request}`);
    }
  }

  throw new Error('2Captcha timeout: solution not received within 120s');
}

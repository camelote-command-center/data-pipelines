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

// ─────────────────────────────────────────────────────────────────────────────
// Friendly Captcha (frc-captcha) solver.
//
// fao.ge.ch migrated from its old GD image CAPTCHA to Friendly Captcha
// (a JS proof-of-work widget). 2Captcha solves it via the createTask API.
// The returned token is injected as the value of input[name="frc-captcha-solution"].
// Docs: https://2captcha.com/api-docs/friendly-captcha
// ─────────────────────────────────────────────────────────────────────────────
const API_CREATE_URL = 'https://api.2captcha.com/createTask';
const API_RESULT_URL = 'https://api.2captcha.com/getTaskResult';
const FRC_POLL_INTERVAL_MS = 5_000;
const FRC_MAX_WAIT_MS = 180_000; // proof-of-work can take longer than image solving

export async function solveFriendlyCaptcha(
  websiteURL: string,
  websiteKey: string,
  version: 'v1' | 'v2' = 'v2',
): Promise<string> {
  const apiKey = process.env.TWO_CAPTCHA_API_KEY;
  if (!apiKey) throw new Error('TWO_CAPTCHA_API_KEY is required');

  const createRes = await fetch(API_CREATE_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      clientKey: apiKey,
      task: {
        type: 'FriendlyCaptchaTaskProxyless',
        websiteURL,
        websiteKey,
        // fao.ge.ch uses Friendly Captcha v2 (data-api-endpoint + createWidget SDK).
        // 2Captcha defaults to v1, whose token fails server verification.
        version,
      },
    }),
  });
  const createData = (await createRes.json()) as {
    errorId: number;
    errorCode?: string;
    errorDescription?: string;
    taskId?: number;
  };

  if (createData.errorId !== 0 || !createData.taskId) {
    throw new Error(
      `2Captcha createTask failed: ${createData.errorCode ?? ''} ${createData.errorDescription ?? ''}`.trim(),
    );
  }

  const taskId = createData.taskId;
  console.log(`  Friendly Captcha task created (id: ${taskId}), polling for solution...`);

  const deadline = Date.now() + FRC_MAX_WAIT_MS;
  while (Date.now() < deadline) {
    await sleep(FRC_POLL_INTERVAL_MS);

    const pollRes = await fetch(API_RESULT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ clientKey: apiKey, taskId }),
    });
    const pollData = (await pollRes.json()) as {
      errorId: number;
      errorCode?: string;
      status?: string;
      solution?: { token?: string };
    };

    if (pollData.errorId !== 0) {
      throw new Error(`2Captcha getTaskResult error: ${pollData.errorCode ?? 'unknown'}`);
    }
    if (pollData.status === 'ready') {
      const token = pollData.solution?.token;
      if (!token) throw new Error('2Captcha returned ready status but no token');
      console.log('  Friendly Captcha solved');
      return token;
    }
    // status === 'processing' -> keep polling
  }

  throw new Error('2Captcha Friendly Captcha timeout: solution not received within 180s');
}

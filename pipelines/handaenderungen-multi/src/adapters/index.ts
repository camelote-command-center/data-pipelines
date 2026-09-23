import type { Canton } from '../types.js';
import type { SourceAdapter } from './types.js';
import { schwyzAdapter } from './schwyz.js';
import { lucerneAdapter } from './lucerne.js';

/** canton → adapter registry. VS intentionally absent (LACC Art. 162 no-go). */
export const ADAPTERS: Record<Canton, SourceAdapter> = {
  SZ: schwyzAdapter,
  LU: lucerneAdapter,
};

export function getAdapter(canton: Canton): SourceAdapter {
  const a = ADAPTERS[canton];
  if (!a) throw new Error(`no adapter for canton ${canton}`);
  return a;
}

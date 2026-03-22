/**
 * Re-exports from _shared for local resolution compatibility.
 * tsx can't always resolve cross-package sibling imports from local node_modules.
 */
export { normalizePhone } from '../_shared/phone.js';

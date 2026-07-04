/**
 * Session pagination constants
 * ============================
 *
 * Single source of truth for the sessions-list page size used by both the
 * backend (projects-cache fallback hasMore derivation) and the frontend
 * (Sidebar lazy "load more" fetch). Keeping them in sync avoids drift where
 * the backend signals "hasMore" at a different threshold than the frontend
 * requests, which would produce overlapping or gapped session pages.
 */

// Number of sessions fetched per page by the sessions detail endpoint and the
// sidebar's lazy-load ("show more") control.
export const SESSION_PAGE_SIZE = 5;

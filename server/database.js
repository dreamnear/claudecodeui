/**
 * SQLite Database Module
 *
 * Single source of truth for all session/project data.
 * Replaces multiple in-memory caches with efficient SQLite storage.
 */

import Database from "better-sqlite3";
import path from "path";
import os from "os";
import fs from "fs";
import { createLogger } from "./logger.js";

const log = createLogger("database");

// Database location
const DB_DIR = path.join(os.homedir(), ".claude", "claudecodeui");
const DB_PATH = path.join(DB_DIR, "cache.db");

// Database instance (singleton)
let db = null;

/**
 * Initialize the database with schema
 */
function initializeSchema(database) {
  database.exec(`
    -- Track file processing state for incremental updates
    CREATE TABLE IF NOT EXISTS file_state (
      file_path TEXT PRIMARY KEY,
      last_byte_offset INTEGER DEFAULT 0,
      last_mtime REAL,
      last_processed_at INTEGER,
      file_size INTEGER DEFAULT 0
    );

    -- Projects metadata
    CREATE TABLE IF NOT EXISTS projects (
      name TEXT PRIMARY KEY,
      display_name TEXT,
      full_path TEXT,
      session_count INTEGER DEFAULT 0,
      last_activity INTEGER,
      has_claude_sessions INTEGER DEFAULT 0,
      has_cursor_sessions INTEGER DEFAULT 0,
      has_codex_sessions INTEGER DEFAULT 0,
      has_taskmaster INTEGER DEFAULT 0,
      updated_at INTEGER
    );

    -- Sessions metadata (lightweight)
    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      project_name TEXT NOT NULL,
      summary TEXT DEFAULT 'New Session',
      message_count INTEGER DEFAULT 0,
      last_activity INTEGER,
      cwd TEXT,
      provider TEXT DEFAULT 'claude',
      is_grouped INTEGER DEFAULT 0,
      group_id TEXT,
      file_path TEXT,
      updated_at INTEGER
    );

    -- Message index (byte offsets for on-demand loading)
    CREATE TABLE IF NOT EXISTS message_index (
      session_id TEXT NOT NULL,
      message_number INTEGER NOT NULL,
      uuid TEXT,
      type TEXT,
      timestamp INTEGER,
      byte_offset INTEGER NOT NULL,
      file_path TEXT NOT NULL,
      PRIMARY KEY (session_id, message_number)
    );

    -- UUID mapping for timeline detection
    CREATE TABLE IF NOT EXISTS uuid_mapping (
      uuid TEXT PRIMARY KEY,
      session_id TEXT NOT NULL,
      parent_uuid TEXT,
      type TEXT
    );

    -- History prompts (from history.jsonl)
    CREATE TABLE IF NOT EXISTS history_prompts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id TEXT NOT NULL,
      prompt TEXT,
      timestamp INTEGER,
      project_path TEXT
    );

    -- Indexes for common queries
    CREATE INDEX IF NOT EXISTS idx_sessions_project ON sessions(project_name);
    CREATE INDEX IF NOT EXISTS idx_sessions_activity ON sessions(last_activity DESC);
    CREATE INDEX IF NOT EXISTS idx_sessions_provider ON sessions(provider);
    CREATE INDEX IF NOT EXISTS idx_messages_session ON message_index(session_id);
    CREATE INDEX IF NOT EXISTS idx_uuid_session ON uuid_mapping(session_id);
    CREATE INDEX IF NOT EXISTS idx_uuid_parent ON uuid_mapping(parent_uuid);
    CREATE INDEX IF NOT EXISTS idx_history_session ON history_prompts(session_id);
    CREATE INDEX IF NOT EXISTS idx_projects_activity ON projects(last_activity DESC);

    -- Version tracking for cache invalidation
    CREATE TABLE IF NOT EXISTS cache_version (
      key TEXT PRIMARY KEY,
      version INTEGER DEFAULT 0,
      updated_at INTEGER
    );
  `);

  // Initialize version counters
  database
    .prepare(
      `
    INSERT OR IGNORE INTO cache_version (key, version, updated_at)
    VALUES ('sessions', 0, ?), ('projects', 0, ?), ('messages', 0, ?)
  `,
    )
    .run(Date.now(), Date.now(), Date.now());
}

/**
 * Get or create database instance
 */
function getDatabase() {
  if (db) return db;

  // Ensure directory exists
  if (!fs.existsSync(DB_DIR)) {
    fs.mkdirSync(DB_DIR, { recursive: true });
  }

  try {
    db = new Database(DB_PATH);
    db.pragma("journal_mode = WAL"); // Better concurrent access
    db.pragma("synchronous = NORMAL"); // Good balance of safety/speed
    db.pragma("cache_size = -64000"); // 64MB page cache
    db.pragma("temp_store = MEMORY");

    initializeSchema(db);
    log.info({ path: DB_PATH }, "Database initialized");

    return db;
  } catch (error) {
    log.error(
      { error: error.message, path: DB_PATH },
      "Failed to open database",
    );
    throw error;
  }
}

/**
 * Close database connection
 */
function closeDatabase() {
  if (db) {
    db.close();
    db = null;
    log.info("Database closed");
  }
}

/**
 * Increment version counter for a cache type
 */
function incrementVersion(key) {
  const database = getDatabase();
  database
    .prepare(
      `
    UPDATE cache_version SET version = version + 1, updated_at = ? WHERE key = ?
  `,
    )
    .run(Date.now(), key);
}

/**
 * Get current version for a cache type
 */
function getVersion(key) {
  const database = getDatabase();
  const row = database
    .prepare(
      `
    SELECT version, updated_at FROM cache_version WHERE key = ?
  `,
    )
    .get(key);
  return row || { version: 0, updated_at: 0 };
}

// ============================================================
// File State Management
// ============================================================

/**
 * Get the processing state for a file
 */
function getFileState(filePath) {
  const database = getDatabase();
  return database
    .prepare(
      `
    SELECT last_byte_offset, last_mtime, last_processed_at, file_size
    FROM file_state WHERE file_path = ?
  `,
    )
    .get(filePath);
}

/**
 * Update file processing state
 */
function updateFileState(filePath, byteOffset, mtime, fileSize) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT OR REPLACE INTO file_state (file_path, last_byte_offset, last_mtime, last_processed_at, file_size)
    VALUES (?, ?, ?, ?, ?)
  `,
    )
    .run(filePath, byteOffset, mtime, Date.now(), fileSize);
}

/**
 * Reset file state (force re-index)
 */
function resetFileState(filePath) {
  const database = getDatabase();
  database.prepare(`DELETE FROM file_state WHERE file_path = ?`).run(filePath);
}

// ============================================================
// Project Operations
// ============================================================

/**
 * Upsert a project
 */
function upsertProject(project) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT INTO projects (name, display_name, full_path, session_count, last_activity,
                         has_claude_sessions, has_cursor_sessions, has_codex_sessions,
                         has_taskmaster, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(name) DO UPDATE SET
      display_name = excluded.display_name,
      full_path = excluded.full_path,
      session_count = excluded.session_count,
      last_activity = excluded.last_activity,
      has_claude_sessions = excluded.has_claude_sessions OR has_claude_sessions,
      has_cursor_sessions = excluded.has_cursor_sessions OR has_cursor_sessions,
      has_codex_sessions = excluded.has_codex_sessions OR has_codex_sessions,
      has_taskmaster = excluded.has_taskmaster OR has_taskmaster,
      updated_at = excluded.updated_at
  `,
    )
    .run(
      project.name,
      project.displayName || project.name,
      project.fullPath || "",
      project.sessionCount || 0,
      project.lastActivity ? new Date(project.lastActivity).getTime() : null,
      project.hasClaudeSessions ? 1 : 0,
      project.hasCursorSessions ? 1 : 0,
      project.hasCodexSessions ? 1 : 0,
      project.hasTaskmaster ? 1 : 0,
      Date.now(),
    );
  incrementVersion("projects");
}

/**
 * Get all projects with optional timeframe filter
 */
function getProjectsFromDb(timeframMs = null) {
  const database = getDatabase();
  let query = `
    SELECT name, display_name as displayName, full_path as fullPath,
           session_count as sessionCount, last_activity as lastActivity,
           has_claude_sessions as hasClaudeSessions,
           has_cursor_sessions as hasCursorSessions,
           has_codex_sessions as hasCodexSessions,
           has_taskmaster as hasTaskmaster
    FROM projects
  `;

  if (timeframMs) {
    const cutoff = Date.now() - timeframMs;
    query += ` WHERE last_activity >= ${cutoff}`;
  }

  query += ` ORDER BY last_activity DESC`;

  return database
    .prepare(query)
    .all()
    .map((row) => ({
      ...row,
      lastActivity: row.lastActivity
        ? new Date(row.lastActivity).toISOString()
        : null,
      hasClaudeSessions: !!row.hasClaudeSessions,
      hasCursorSessions: !!row.hasCursorSessions,
      hasCodexSessions: !!row.hasCodexSessions,
      hasTaskmaster: !!row.hasTaskmaster,
    }));
}

/**
 * Update project session count and last activity.
 * session_count is the raw session count from the sessions table (COUNT(*)).
 * The timeline view groups sessions server-side (server/projects.js), so the
 * badge number may be slightly higher than the grouped entries the user sees
 * (over-count only, never under-count). last_activity is derived from the
 * sessions table MAX, symmetric to indexProject's getLastActivityByProject.
 */
function updateProjectSessionCount(projectName) {
  const database = getDatabase();
  const sessionCount = getSessionCountByProject(projectName);
  const row = database
    .prepare(
      "SELECT MAX(last_activity) AS last_activity FROM sessions WHERE project_name = ?",
    )
    .get(projectName);
  database
    .prepare(
      `UPDATE projects
       SET session_count = ?, last_activity = ?, updated_at = ?
       WHERE name = ?`,
    )
    .run(sessionCount, row?.last_activity ?? null, Date.now(), projectName);
  incrementVersion("projects");
}

// ============================================================
// Session Operations
// ============================================================

/**
 * Upsert a session.
 *
 * @param {object} session - Session row fields (see INSERT column list).
 * @param {object} [options]
 * @param {boolean} [options.skipProjectAggregate=false] - When true, skip the
 *   per-call `updateProjectSessionCount` refresh. Batch indexing writes many
 *   sessions for one project in a row; recomputing the project's
 *   COUNT(*) + MAX(...) after every write is O(N²). The caller is
 *   expected to refresh the project aggregate once at the end of the batch
 *   (see db-indexer.js indexProject -> upsertProject). The watcher path
 *   (indexFile) keeps the default behavior so single-file increments still
 *   refresh the project row immediately.
 */
function upsertSession(session, options = {}) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT INTO sessions (id, project_name, summary, message_count, last_activity,
                         cwd, provider, is_grouped, group_id, file_path, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      summary = CASE WHEN excluded.summary != 'New Session' THEN excluded.summary ELSE summary END,
      message_count = CASE WHEN excluded.message_count > message_count
                          THEN excluded.message_count ELSE message_count END,
      last_activity = CASE WHEN excluded.last_activity > last_activity
                          THEN excluded.last_activity ELSE last_activity END,
      cwd = COALESCE(excluded.cwd, cwd),
      file_path = COALESCE(excluded.file_path, file_path),
      updated_at = excluded.updated_at
  `,
    )
    .run(
      session.id,
      session.projectName,
      session.summary || "New Session",
      session.messageCount || 0,
      session.lastActivity ? new Date(session.lastActivity).getTime() : null,
      session.cwd || null,
      session.provider || "claude",
      session.isGrouped ? 1 : 0,
      session.groupId || null,
      session.filePath || null,
      Date.now(),
    );
  incrementVersion("sessions");
  if (!options.skipProjectAggregate) {
    updateProjectSessionCount(session.projectName);
  }
}

/**
 * Get session count for a project
 */
function getSessionCountByProject(projectName) {
  const database = getDatabase();
  const result = database
    .prepare("SELECT COUNT(*) as count FROM sessions WHERE project_name = ?")
    .get(projectName);
  return result.count;
}

/**
 * Get the most recent session activity timestamp for a project.
 * Derived from the authoritative sessions table so it stays accurate even when
 * processSessionFile skips unchanged files (incremental indexing).
 */
function getLastActivityByProject(projectName) {
  const database = getDatabase();
  const result = database
    .prepare(
      "SELECT MAX(last_activity) as last_activity FROM sessions WHERE project_name = ?",
    )
    .get(projectName);
  const ts = result?.last_activity;
  return ts ? new Date(ts).toISOString() : null;
}

/**
 * Get sessions with optional filters
 */
function getSessions(options = {}) {
  const database = getDatabase();
  const {
    projectName,
    timeframMs,
    limit = 100,
    offset = 0,
    provider,
  } = options;

  let query = `
    SELECT s.id, s.project_name as projectName, s.summary, s.message_count as messageCount,
           s.last_activity as lastActivity, s.cwd, s.provider, s.is_grouped as isGrouped,
           s.group_id as groupId,
           p.display_name as projectDisplayName, p.full_path as projectFullPath
    FROM sessions s
    LEFT JOIN projects p ON s.project_name = p.name
    WHERE 1=1
  `;

  const params = [];

  if (projectName) {
    query += ` AND s.project_name = ?`;
    params.push(projectName);
  }

  if (timeframMs) {
    const cutoff = Date.now() - timeframMs;
    query += ` AND s.last_activity >= ?`;
    params.push(cutoff);
  }

  if (provider) {
    query += ` AND s.provider = ?`;
    params.push(provider);
  }

  query += ` ORDER BY s.last_activity DESC LIMIT ? OFFSET ?`;
  params.push(limit, offset);

  return database
    .prepare(query)
    .all(...params)
    .map((row) => ({
      ...row,
      lastActivity: row.lastActivity
        ? new Date(row.lastActivity).toISOString()
        : null,
      isGrouped: !!row.isGrouped,
      project: {
        name: row.projectName,
        displayName: row.projectDisplayName,
        fullPath: row.projectFullPath,
      },
    }));
}

/**
 * Get session count
 */
function getSessionCount(options = {}) {
  const database = getDatabase();
  const { projectName, timeframMs, provider } = options;

  let query = `SELECT COUNT(*) as count FROM sessions WHERE 1=1`;
  const params = [];

  if (projectName) {
    query += ` AND project_name = ?`;
    params.push(projectName);
  }

  if (timeframMs) {
    const cutoff = Date.now() - timeframMs;
    query += ` AND last_activity >= ?`;
    params.push(cutoff);
  }

  if (provider) {
    query += ` AND provider = ?`;
    params.push(provider);
  }

  return database.prepare(query).get(...params).count;
}

/**
 * Get session by ID
 */
function getSession(sessionId) {
  const database = getDatabase();
  const row = database
    .prepare(
      `
    SELECT s.*, p.display_name as projectDisplayName, p.full_path as projectFullPath
    FROM sessions s
    LEFT JOIN projects p ON s.project_name = p.name
    WHERE s.id = ?
  `,
    )
    .get(sessionId);

  if (!row) return null;

  return {
    id: row.id,
    projectName: row.project_name,
    summary: row.summary,
    messageCount: row.message_count,
    lastActivity: row.last_activity
      ? new Date(row.last_activity).toISOString()
      : null,
    cwd: row.cwd,
    provider: row.provider,
    filePath: row.file_path,
    project: {
      name: row.project_name,
      displayName: row.projectDisplayName,
      fullPath: row.projectFullPath,
    },
  };
}

/**
 * Update session summary
 */
function updateSessionSummary(sessionId, summary) {
  const database = getDatabase();
  database
    .prepare(
      `
    UPDATE sessions SET summary = ?, updated_at = ? WHERE id = ?
  `,
    )
    .run(summary, Date.now(), sessionId);
  incrementVersion("sessions");
}

// ============================================================
// Message Index Operations
// ============================================================

/**
 * Insert message index entry
 */
function insertMessageIndex(entry) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT OR REPLACE INTO message_index (session_id, message_number, uuid, type, timestamp, byte_offset, file_path)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `,
    )
    .run(
      entry.sessionId,
      entry.messageNumber,
      entry.uuid || null,
      entry.type || null,
      entry.timestamp ? new Date(entry.timestamp).getTime() : null,
      entry.byteOffset,
      entry.filePath,
    );
}

/**
 * Bulk insert message indexes (for efficiency)
 */
function insertMessageIndexBatch(entries) {
  const database = getDatabase();
  const insert = database.prepare(`
    INSERT OR REPLACE INTO message_index (session_id, message_number, uuid, type, timestamp, byte_offset, file_path)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  const insertMany = database.transaction((entries) => {
    for (const entry of entries) {
      insert.run(
        entry.sessionId,
        entry.messageNumber,
        entry.uuid || null,
        entry.type || null,
        entry.timestamp ? new Date(entry.timestamp).getTime() : null,
        entry.byteOffset,
        entry.filePath,
      );
    }
  });

  insertMany(entries);
  incrementVersion("messages");
}

/**
 * Get message index entry
 */
function getMessageIndex(sessionId, messageNumber) {
  const database = getDatabase();
  return database
    .prepare(
      `
    SELECT * FROM message_index WHERE session_id = ? AND message_number = ?
  `,
    )
    .get(sessionId, messageNumber);
}

/**
 * Get message list for a session
 */
function getMessageListFromDb(sessionId) {
  const database = getDatabase();
  return database
    .prepare(
      `
    SELECT message_number as number, uuid as id, type, timestamp
    FROM message_index
    WHERE session_id = ?
    ORDER BY message_number ASC
  `,
    )
    .all(sessionId)
    .map((row) => ({
      ...row,
      timestamp: row.timestamp ? new Date(row.timestamp).toISOString() : null,
    }));
}

/**
 * Get message count for a session
 */
function getMessageCountFromDb(sessionId) {
  const database = getDatabase();
  const row = database
    .prepare(
      `
    SELECT COUNT(*) as count FROM message_index WHERE session_id = ?
  `,
    )
    .get(sessionId);
  return row ? row.count : 0;
}

/**
 * Delete message indexes for a session (for re-indexing)
 */
function deleteSessionMessageIndexes(sessionId) {
  const database = getDatabase();
  database
    .prepare(`DELETE FROM message_index WHERE session_id = ?`)
    .run(sessionId);
}

// ============================================================
// UUID Mapping Operations
// ============================================================

/**
 * Insert UUID mapping
 */
function insertUuidMapping(uuid, sessionId, parentUuid, type) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT OR REPLACE INTO uuid_mapping (uuid, session_id, parent_uuid, type)
    VALUES (?, ?, ?, ?)
  `,
    )
    .run(uuid, sessionId, parentUuid, type);
}

/**
 * Bulk insert UUID mappings
 */
function insertUuidMappingBatch(mappings) {
  const database = getDatabase();
  const insert = database.prepare(`
    INSERT OR REPLACE INTO uuid_mapping (uuid, session_id, parent_uuid, type)
    VALUES (?, ?, ?, ?)
  `);

  const insertMany = database.transaction((mappings) => {
    for (const m of mappings) {
      insert.run(m.uuid, m.sessionId, m.parentUuid || null, m.type || null);
    }
  });

  insertMany(mappings);
}

/**
 * Get session ID for a UUID
 */
function getSessionIdForUuid(uuid) {
  const database = getDatabase();
  const row = database
    .prepare(
      `
    SELECT session_id FROM uuid_mapping WHERE uuid = ?
  `,
    )
    .get(uuid);
  return row ? row.session_id : null;
}

/**
 * Get first user messages (for timeline grouping)
 */
function getFirstUserMessages(projectName = null) {
  const database = getDatabase();
  let query = `
    SELECT u.uuid, u.session_id as sessionId
    FROM uuid_mapping u
    WHERE u.parent_uuid IS NULL AND u.type = 'user'
  `;

  if (projectName) {
    query += `
      AND EXISTS (SELECT 1 FROM sessions s WHERE s.id = u.session_id AND s.project_name = ?)
    `;
    return database.prepare(query).all(projectName);
  }

  return database.prepare(query).all();
}

// ============================================================
// History Prompts Operations
// ============================================================

/**
 * Insert history prompt
 */
function insertHistoryPrompt(sessionId, prompt, timestamp, projectPath) {
  const database = getDatabase();
  database
    .prepare(
      `
    INSERT INTO history_prompts (session_id, prompt, timestamp, project_path)
    VALUES (?, ?, ?, ?)
  `,
    )
    .run(sessionId, prompt, timestamp, projectPath);
}

/**
 * Get prompts for a session
 */
function getSessionPromptsFromDb(sessionId) {
  const database = getDatabase();
  return database
    .prepare(
      `
    SELECT prompt, timestamp, project_path as projectPath
    FROM history_prompts
    WHERE session_id = ?
    ORDER BY timestamp ASC
  `,
    )
    .all(sessionId);
}

/**
 * Clear history prompts (for re-indexing)
 */
function clearHistoryPrompts() {
  const database = getDatabase();
  database.prepare(`DELETE FROM history_prompts`).run();
}

// ============================================================
// Utility Operations
// ============================================================

/**
 * Get a project's cwd from its sessions
 * Useful as a fallback when session files are skipped during indexing
 */
function getProjectCwdFromSessions(projectName) {
  const database = getDatabase();
  const row = database
    .prepare(
      `SELECT cwd FROM sessions WHERE project_name = ? AND cwd IS NOT NULL AND cwd != '' LIMIT 1`,
    )
    .get(projectName);
  return row ? row.cwd : null;
}

/**
 * Get database statistics
 */
function getStats() {
  const database = getDatabase();
  return {
    projects: database.prepare(`SELECT COUNT(*) as count FROM projects`).get()
      .count,
    sessions: database.prepare(`SELECT COUNT(*) as count FROM sessions`).get()
      .count,
    messageIndexes: database
      .prepare(`SELECT COUNT(*) as count FROM message_index`)
      .get().count,
    uuidMappings: database
      .prepare(`SELECT COUNT(*) as count FROM uuid_mapping`)
      .get().count,
    historyPrompts: database
      .prepare(`SELECT COUNT(*) as count FROM history_prompts`)
      .get().count,
    versions: {
      sessions: getVersion("sessions"),
      projects: getVersion("projects"),
      messages: getVersion("messages"),
    },
  };
}

/**
 * Clear all data (for testing or reset)
 */
function clearAllData() {
  const database = getDatabase();
  database.exec(`
    DELETE FROM message_index;
    DELETE FROM uuid_mapping;
    DELETE FROM history_prompts;
    DELETE FROM sessions;
    DELETE FROM projects;
    DELETE FROM file_state;
  `);
  database
    .prepare(`UPDATE cache_version SET version = 0, updated_at = ?`)
    .run(Date.now());
  log.info("All data cleared");
}

export {
  getDatabase,
  closeDatabase,
  incrementVersion,
  getVersion,
  // File state
  getFileState,
  updateFileState,
  resetFileState,
  // Projects
  upsertProject,
  getProjectsFromDb,
  updateProjectSessionCount,
  // Sessions
  upsertSession,
  getSessions,
  getSessionCount,
  getSessionCountByProject,
  getLastActivityByProject,
  getSession,
  updateSessionSummary,
  getProjectCwdFromSessions,
  // Message index
  insertMessageIndex,
  insertMessageIndexBatch,
  getMessageIndex,
  getMessageListFromDb,
  getMessageCountFromDb,
  deleteSessionMessageIndexes,
  // UUID mapping
  insertUuidMapping,
  insertUuidMappingBatch,
  getSessionIdForUuid,
  getFirstUserMessages,
  // History prompts
  insertHistoryPrompt,
  getSessionPromptsFromDb,
  clearHistoryPrompts,
  // Utility
  getStats,
  clearAllData,
  DB_PATH,
};

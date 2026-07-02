/**
 * Database Indexer Module
 *
 * Incrementally indexes session files into SQLite.
 * Only processes new bytes when files are appended.
 */

import fs from "fs";
import fsPromises from "fs/promises";
import path from "path";
import os from "os";
import readline from "readline";
import { createLogger } from "./logger.js";
import {
  getFileState,
  updateFileState,
  upsertProject,
  upsertSession,
  insertMessageIndexBatch,
  insertUuidMappingBatch,
  deleteSessionMessageIndexes,
  getStats,
  getProjectCwdFromSessions,
  getSessionCountByProject,
} from "./database.js";

const log = createLogger("db-indexer");

// Claude projects path
const CLAUDE_PROJECTS_PATH = path.join(os.homedir(), ".claude", "projects");

/**
 * Process a single session file incrementally
 * Only reads new bytes since last processing
 */
async function processSessionFile(filePath, projectName) {
  try {
    const stats = await fsPromises.stat(filePath);
    const fileState = getFileState(filePath);

    // Check if file has changed
    if (fileState && fileState.last_mtime === stats.mtimeMs) {
      return { skipped: true, reason: "unchanged" };
    }

    // Check if we can do incremental update (file grew)
    const isIncremental =
      fileState &&
      fileState.file_size &&
      stats.size > fileState.file_size &&
      fileState.last_mtime < stats.mtimeMs;

    const startOffset = isIncremental ? fileState.last_byte_offset : 0;

    // Extract session ID from filename
    const sessionId = path.basename(filePath, ".jsonl");

    // If starting fresh, clear existing indexes for this session
    if (!isIncremental) {
      deleteSessionMessageIndexes(sessionId);
    }

    const entries = [];
    const uuidMappings = [];
    const messageIndexes = [];

    let byteOffset = startOffset;
    let messageNumber = isIncremental ? fileState.message_count || 0 : 0;
    let lastActivity = null;
    let summary = "New Session";
    let firstUserMessage = null;
    let cwd = null;

    // Stream the file from the start offset
    const stream = fs.createReadStream(filePath, {
      start: startOffset,
      encoding: "utf8",
    });

    const rl = readline.createInterface({
      input: stream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      const lineBytes = Buffer.byteLength(line, "utf8") + 1; // +1 for newline

      if (line.trim()) {
        try {
          const entry = JSON.parse(line);

          // Handle summary entries (they don't have sessionId)
          if (entry.type === "summary" && entry.summary) {
            summary = entry.summary;
          }

          // Capture first user message as fallback title
          if (
            !firstUserMessage &&
            entry.type === "user" &&
            entry.message?.content
          ) {
            const content = entry.message.content;
            // Handle both string and array content formats
            if (typeof content === "string") {
              firstUserMessage = content;
            } else if (Array.isArray(content) && content[0]?.text) {
              firstUserMessage = content[0].text;
            }
          }

          if (entry.sessionId === sessionId) {
            messageNumber++;

            // Build message index
            messageIndexes.push({
              sessionId,
              messageNumber,
              uuid: entry.uuid || null,
              type: entry.type || null,
              timestamp: entry.timestamp,
              byteOffset: byteOffset,
              filePath,
            });

            // Build UUID mapping
            if (entry.uuid) {
              uuidMappings.push({
                uuid: entry.uuid,
                sessionId,
                parentUuid: entry.parentUuid || null,
                type: entry.type || null,
              });
            }

            // Track session metadata
            if (entry.timestamp) {
              const ts = new Date(entry.timestamp);
              if (!lastActivity || ts > lastActivity) {
                lastActivity = ts;
              }
            }

            // Extract cwd
            if (entry.cwd && !cwd) {
              cwd = entry.cwd;
            }
          }
        } catch (parseError) {
          // Skip malformed lines
        }
      }

      byteOffset += lineBytes;
    }

    // Batch insert indexes
    if (messageIndexes.length > 0) {
      insertMessageIndexBatch(messageIndexes);
    }

    if (uuidMappings.length > 0) {
      insertUuidMappingBatch(uuidMappings);
    }

    // Use first user message as fallback if no summary
    let finalSummary = summary;
    if (summary === "New Session" && firstUserMessage) {
      // Truncate long messages and clean up for display
      finalSummary = firstUserMessage
        .replace(/\n/g, " ")
        .replace(/\s+/g, " ")
        .trim();
      if (finalSummary.length > 100) {
        finalSummary = finalSummary.substring(0, 97) + "...";
      }
    }

    // Update session metadata
    upsertSession({
      id: sessionId,
      projectName,
      summary: finalSummary,
      messageCount: messageNumber,
      lastActivity: lastActivity ? lastActivity.toISOString() : null,
      cwd,
      provider: "claude",
      filePath,
    });

    // Update file state
    updateFileState(filePath, byteOffset, stats.mtimeMs, stats.size);

    return {
      skipped: false,
      sessionId,
      messagesIndexed: messageIndexes.length,
      isIncremental,
      totalMessages: messageNumber,
      cwd,
    };
  } catch (error) {
    log.error({ error: error.message, filePath }, "Error processing file");
    return { skipped: true, reason: "error", error: error.message };
  }
}

/**
 * Get display name for a project by trying package.json first, then path-based
 * @param {string} actualPath - The actual filesystem path to the project
 */
async function getProjectDisplayName(actualPath) {
  if (!actualPath) {
    return null;
  }

  // Try to read package.json
  try {
    const packageJsonPath = path.join(actualPath, "package.json");
    const packageData = await fsPromises.readFile(packageJsonPath, "utf8");
    const packageJson = JSON.parse(packageData);
    if (packageJson.name) {
      return packageJson.name;
    }
  } catch {
    // Fall back to path-based naming
  }

  // Return last 2 parts of the actual path like "org/repo"
  const parts = actualPath.split("/").filter(Boolean);
  if (parts.length >= 2) {
    return parts.slice(-2).join("/");
  }
  return parts.pop() || actualPath;
}

/**
 * Index all files in a project directory
 */
async function indexProject(projectDir) {
  const projectName = path.basename(projectDir);

  try {
    const files = await fsPromises.readdir(projectDir);
    const jsonlFiles = files.filter(
      (f) => f.endsWith(".jsonl") && !f.startsWith("agent-"),
    );

    let lastActivity = null;
    let sessionCount = 0;
    let projectCwd = null;
    const results = [];

    for (const file of jsonlFiles) {
      const filePath = path.join(projectDir, file);
      const result = await processSessionFile(filePath, projectName);
      results.push(result);

      if (!result.skipped) {
        sessionCount++;
        // Track project's last activity
        if (result.lastActivity) {
          const ts = new Date(result.lastActivity);
          if (!lastActivity || ts > lastActivity) {
            lastActivity = ts;
          }
        }
        // Capture the first valid cwd for display name generation
        if (!projectCwd && result.cwd) {
          projectCwd = result.cwd;
        }
      }
    }

    // If no cwd was found from processing (e.g., all files skipped), try database
    if (!projectCwd) {
      projectCwd = getProjectCwdFromSessions(projectName);
    }

    // Generate display name from actual path (from session cwd)
    const displayName = await getProjectDisplayName(projectCwd);

    // Use real session count from database, not the counter
    const realSessionCount = getSessionCountByProject(projectName);

    upsertProject({
      name: projectName,
      displayName: displayName || decodeProjectName(projectName),
      fullPath: projectCwd || projectDir,
      sessionCount: realSessionCount,
      lastActivity: lastActivity ? lastActivity.toISOString() : null,
      hasClaudeSessions: realSessionCount > 0,
    });

    return { projectName, filesProcessed: results.length, results };
  } catch (error) {
    log.error({ error: error.message, projectDir }, "Error indexing project");
    return { projectName, error: error.message };
  }
}

/**
 * Index all projects in the Claude projects directory
 */
async function indexAllProjects() {
  const startTime = Date.now();

  try {
    if (!fs.existsSync(CLAUDE_PROJECTS_PATH)) {
      log.warn({ path: CLAUDE_PROJECTS_PATH }, "Projects path does not exist");
      return { success: false, error: "Projects path not found" };
    }

    const entries = await fsPromises.readdir(CLAUDE_PROJECTS_PATH, {
      withFileTypes: true,
    });

    const projectDirs = entries
      .filter((e) => e.isDirectory())
      .map((e) => path.join(CLAUDE_PROJECTS_PATH, e.name));

    const results = [];
    for (const dir of projectDirs) {
      const result = await indexProject(dir);
      results.push(result);
    }

    const duration = Date.now() - startTime;
    const stats = getStats();

    log.info(
      {
        projectsIndexed: results.length,
        durationMs: duration,
        stats,
      },
      "Full indexing complete",
    );

    return {
      success: true,
      projectsIndexed: results.length,
      durationMs: duration,
      stats,
    };
  } catch (error) {
    log.error({ error: error.message }, "Error during full indexing");
    return { success: false, error: error.message };
  }
}

/**
 * Index a single file (called on file change)
 */
async function indexFile(filePath) {
  // Extract project name from path
  const projectDir = path.dirname(filePath);
  const projectName = path.basename(projectDir);

  // Check this is in the projects directory
  if (!filePath.startsWith(CLAUDE_PROJECTS_PATH)) {
    return { skipped: true, reason: "not in projects directory" };
  }

  // Only process .jsonl files
  if (
    !filePath.endsWith(".jsonl") ||
    path.basename(filePath).startsWith("agent-")
  ) {
    return { skipped: true, reason: "not a session file" };
  }

  return processSessionFile(filePath, projectName);
}

/**
 * Decode project name from URL-encoded format
 * Returns a meaningful display name (last 2 path parts like "org/repo")
 */
function decodeProjectName(encodedName) {
  try {
    // Replace - with / for path reconstruction
    const decoded = decodeURIComponent(encodedName.replace(/-/g, "/"));
    const parts = decoded.split("/").filter(Boolean);

    // Return last 2 parts for more context (e.g., "epiphytic/claudecodeui")
    // Skip common parent dirs like "repos", "projects", "src"
    const skipParts = ["repos", "projects", "src", "code", "Users", "home"];
    const meaningfulParts = parts.filter((p) => !skipParts.includes(p));

    if (meaningfulParts.length >= 2) {
      return meaningfulParts.slice(-2).join("/");
    }
    return parts.pop() || encodedName;
  } catch {
    return encodedName;
  }
}

export {
  processSessionFile,
  indexProject,
  indexAllProjects,
  indexFile,
  CLAUDE_PROJECTS_PATH,
};

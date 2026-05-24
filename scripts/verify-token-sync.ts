import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { closePool, getPool, recordRound } from "../src/database.js";

const execFileAsync = promisify(execFile);
const projectPath = "/Users/dubo/Documents/sbt/sl/mcp";

try {
  const latestUsage = await latestCodexUsage(projectPath);
  if (!latestUsage) {
    console.log(JSON.stringify({
      ok: true,
      skipped: true,
      reason: "No recent Codex token usage log found"
    }, null, 2));
    process.exit(0);
  }

  const startedAt = new Date((latestUsage.ts - 30) * 1000);
  const endedAt = new Date((latestUsage.ts + 30) * 1000);
  const round = await recordRound({
    conversationId: `codex:${projectPath}`,
    startedAt: startedAt.toISOString(),
    endedAt: endedAt.toISOString(),
    modelName: latestUsage.modelName ?? "gpt-5.5",
    promptText: "#999 token sync verification",
    filesChanged: 0,
    linesAdded: 0,
    linesDeleted: 0,
    inputTokens: 0,
    outputTokens: 0,
    totalTokens: 0,
    metadata: {
      client: "codex",
      projectPath,
      threadId: latestUsage.threadId,
      turnId: latestUsage.turnId,
      tokenStatsUnavailable: true
    }
  });

  await execFileAsync("npm", [
    "run",
    "tokens:sync",
    "--",
    "--client",
    "codex",
    "--round-id",
    String(round.id),
    "--project",
    projectPath
  ], {
    cwd: projectPath,
    maxBuffer: 20 * 1024 * 1024
  });

  const [rows] = await getPool().execute<any[]>(
    `SELECT id, total_tokens, token_source, token_sync_status, token_sync_note
     FROM ai_coding_rounds
     WHERE id = :id`,
    { id: round.id }
  );

  const updated = rows[0];
  if (!updated || updated.token_sync_status !== "synced" || Number(updated.total_tokens) <= 0) {
    throw new Error(`Expected round ${round.id} to sync token usage, got ${JSON.stringify(updated)}`);
  }

  console.log(JSON.stringify({
    ok: true,
    roundId: round.id,
    totalTokens: Number(updated.total_tokens),
    tokenSource: updated.token_source,
    tokenSyncStatus: updated.token_sync_status,
    tokenSyncNote: updated.token_sync_note
  }, null, 2));
} finally {
  await closePool();
}

async function latestCodexUsage(projectPath: string): Promise<{ ts: number; modelName?: string; threadId?: string; turnId?: string } | null> {
  const threadId = await latestCodexThreadId(projectPath);
  const sql = `
    SELECT ts, feedback_log_body
    FROM logs
    WHERE ts >= ${Math.floor(Date.now() / 1000) - 6 * 60 * 60}
      AND target = 'codex_core::session::turn'
      AND feedback_log_body LIKE '%:run_turn: post sampling token usage%'
      AND feedback_log_body LIKE '%thread.id=${threadId}%'
    ORDER BY id DESC
    LIMIT 1
  `;
  const { stdout } = await execFileAsync("sqlite3", [
    "-json",
    "/Users/dubo/.codex/logs_2.sqlite",
    sql
  ], {
    maxBuffer: 20 * 1024 * 1024
  });

  const rows = stdout.trim() ? JSON.parse(stdout) : [];
  const row = rows[0];
  if (!row) return null;

  return {
    ts: Number(row.ts),
    modelName: String(row.feedback_log_body ?? "").match(/model=([^}: ]+)/)?.[1],
    threadId,
    turnId: String(row.feedback_log_body ?? "").match(/turn_id=([0-9a-f-]+)/)?.[1]
  };
}

async function latestCodexThreadId(projectPath: string): Promise<string> {
  const { stdout } = await execFileAsync("sqlite3", [
    "-json",
    "/Users/dubo/.codex/state_5.sqlite",
    `SELECT id FROM threads WHERE cwd = '${projectPath.replaceAll("'", "''")}' ORDER BY updated_at_ms DESC, updated_at DESC LIMIT 1`
  ], {
    maxBuffer: 1024 * 1024
  });
  const rows = stdout.trim() ? JSON.parse(stdout) : [];
  const id = rows[0]?.id;
  if (!id) {
    throw new Error(`No Codex thread found for ${projectPath}`);
  }
  return String(id);
}

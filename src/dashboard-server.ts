import { createHmac, timingSafeEqual } from "node:crypto";
import { createReadStream } from "node:fs";
import { open, readdir, stat } from "node:fs/promises";
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { homedir } from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { PoolConnection, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { getPool } from "./database.js";
import { getDashboardConfig, DashboardConfig } from "./dashboard-config.js";

type QueryValue = string | number | null;
type QueryParams = Record<string, QueryValue>;

type FilterOptions = {
  from?: string;
  to?: string;
  model?: string;
  requirementId?: string;
  client?: string;
  includeReverted: boolean;
};

type RequirementRecordInput = {
  requirementId: number;
  title?: string | null;
  projectName?: string | null;
  gpmNumber?: string | null;
  status?: string | null;
  description?: string | null;
};

type RoundRecordInput = {
  requirementId: number | null;
  requirementSource: "prompt" | "context" | "empty";
  modelName: string;
  startedAt: string;
  endedAt: string;
  promptText: string | null;
  filesChanged: number | null;
  linesAdded: number;
  linesDeleted: number;
  codeLinesChanged: number;
  inputTokens: number;
  outputTokens: number;
  totalTokens: number;
  client: string | null;
};

const moduleDir = path.dirname(fileURLToPath(import.meta.url));
const staticRoot = path.resolve(moduleDir, "..", "public", "dashboard");
const cookieName = "ai_coding_dashboard_session";
const maxLogFilesReturned = 200;
const maxLogFilesScanned = 3000;
const maxLogTailBytes = 256 * 1024;
const defaultLogTailBytes = 64 * 1024;

const mimeTypes: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon"
};

export function createDashboardServer(config: DashboardConfig = getDashboardConfig()) {
  return createServer(async (request, response) => {
    try {
      await routeRequest(request, response, config);
    } catch (error) {
      console.error(error);
      sendJson(response, 500, {
        error: "internal_error",
        message: error instanceof Error ? error.message : "Unknown error"
      });
    }
  });
}

async function routeRequest(
  request: IncomingMessage,
  response: ServerResponse,
  config: DashboardConfig
): Promise<void> {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);

  if (request.method === "GET" && url.pathname === "/health") {
    sendJson(response, 200, { ok: true });
    return;
  }

  if (request.method === "GET" && url.pathname === "/login") {
    await sendStatic(response, "login.html");
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/login") {
    await handleLogin(request, response, config);
    return;
  }

  if (request.method === "POST" && url.pathname === "/api/logout") {
    clearSessionCookie(response);
    sendJson(response, 200, { ok: true });
    return;
  }

  const authenticated = isAuthenticated(request, config);

  if (request.method === "GET" && url.pathname === "/api/session") {
    sendJson(response, authenticated ? 200 : 401, { authenticated });
    return;
  }

  if (!authenticated) {
    if (url.pathname.startsWith("/api/")) {
      sendJson(response, 401, { error: "unauthorized" });
      return;
    }

    redirect(response, "/login");
    return;
  }

  if (request.method === "GET" && url.pathname === "/") {
    await sendStatic(response, "index.html");
    return;
  }

  if (url.pathname.startsWith("/api/")) {
    await handleApi(request, url, response);
    return;
  }

  if (request.method === "GET") {
    const relativePath = url.pathname.replace(/^\/+/, "");
    await sendStatic(response, relativePath || "index.html");
    return;
  }

  sendJson(response, 405, { error: "method_not_allowed" });
}

async function handleApi(
  request: IncomingMessage,
  url: URL,
  response: ServerResponse
): Promise<void> {
  const filters = parseFilters(url.searchParams);

  if (request.method === "GET" && url.pathname === "/api/summary") {
    sendJson(response, 200, await getSummary(filters));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/requirements") {
    sendJson(response, 200, await getRequirements(filters));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/models") {
    sendJson(response, 200, await getModels(filters));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/timeline") {
    sendJson(response, 200, await getTimeline(filters));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/rounds") {
    const limit = Math.min(Math.max(Number(url.searchParams.get("limit") ?? 50), 1), 200);
    sendJson(response, 200, await getRounds(filters, limit));
    return;
  }

  const roundMatch = url.pathname.match(/^\/api\/rounds\/([1-9]\d*)$/);
  if (roundMatch && request.method === "PUT") {
    const body = await readJsonBody(request);
    sendJson(response, 200, await updateRoundRecord(Number(roundMatch[1]), body));
    return;
  }

  if (roundMatch && request.method === "DELETE") {
    sendJson(response, 200, await deleteRoundRecord(Number(roundMatch[1])));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/filters") {
    sendJson(response, 200, await getFilters());
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/local-logs/files") {
    sendJson(response, 200, await getLocalLogFiles(url.searchParams));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/local-logs/file") {
    sendJson(response, 200, await getLocalLogFile(url.searchParams));
    return;
  }

  if (request.method === "GET" && url.pathname === "/api/requirement-records") {
    sendJson(response, 200, await getRequirementRecords());
    return;
  }

  const requirementMatch = url.pathname.match(/^\/api\/requirement-records\/([1-9]\d*)$/);
  if (requirementMatch && request.method === "PUT") {
    const body = await readJsonBody(request);
    const requirementId = Number(requirementMatch[1]);
    sendJson(response, 200, await saveRequirementRecord({ ...body, requirementId }));
    return;
  }

  if (requirementMatch && request.method === "DELETE") {
    sendJson(response, 200, await deleteRequirementRecord(Number(requirementMatch[1])));
    return;
  }

  if (url.pathname.startsWith("/api/requirement-records")) {
    sendJson(response, 405, { error: "method_not_allowed" });
    return;
  }

  sendJson(response, 404, { error: "not_found" });
}

async function getSummary(filters: FilterOptions) {
  const { fromSql, whereSql, params } = buildRoundSource(filters);
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       COUNT(DISTINCT requirement_id) AS requirementCount,
       SUM(CASE WHEN requirement_id IS NULL THEN 1 ELSE 0 END) AS unlinkedRounds,
       COUNT(*) AS roundCount,
       COALESCE(SUM(total_tokens), 0) AS totalTokens,
       COALESCE(SUM(code_lines_changed), 0) AS codeLinesChanged,
       COALESCE(SUM(duration_ms), 0) AS durationMs,
       SUM(CASE WHEN total_tokens = 0 THEN 1 ELSE 0 END) AS tokenMissingRounds,
       COALESCE(SUM(CASE WHEN total_tokens > 0 THEN code_lines_changed ELSE 0 END), 0) AS tokenMeasuredCodeLines,
       COALESCE(SUM(CASE WHEN total_tokens > 0 THEN total_tokens ELSE 0 END), 0) AS tokenMeasuredTokens,
       SUM(CASE WHEN token_sync_status = 'synced' THEN 1 ELSE 0 END) AS tokenSyncedRounds,
       SUM(CASE WHEN token_source = 'claude_jsonl' THEN 1 ELSE 0 END) AS claudeTokenRounds,
       SUM(CASE WHEN token_source = 'codex_log' THEN 1 ELSE 0 END) AS codexTokenRounds,
       SUM(CASE WHEN token_sync_status IN ('not_found','ambiguous','failed') THEN 1 ELSE 0 END) AS tokenSyncIssueRounds,
       SUM(CASE WHEN is_reverted = 1 THEN 1 ELSE 0 END) AS revertedRounds
     ${fromSql}
     ${whereSql}`,
    params
  );

  const row = rows[0] ?? {};
  const tokenMeasuredTokens = Number(row.tokenMeasuredTokens ?? 0);
  const tokenMeasuredCodeLines = Number(row.tokenMeasuredCodeLines ?? 0);

  return {
    requirementCount: Number(row.requirementCount ?? 0),
    unlinkedRounds: Number(row.unlinkedRounds ?? 0),
    roundCount: Number(row.roundCount ?? 0),
    totalTokens: Number(row.totalTokens ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    durationMs: Number(row.durationMs ?? 0),
    tokenMissingRounds: Number(row.tokenMissingRounds ?? 0),
    revertedRounds: Number(row.revertedRounds ?? 0),
    tokenSyncedRounds: Number(row.tokenSyncedRounds ?? 0),
    claudeTokenRounds: Number(row.claudeTokenRounds ?? 0),
    codexTokenRounds: Number(row.codexTokenRounds ?? 0),
    tokenSyncIssueRounds: Number(row.tokenSyncIssueRounds ?? 0),
    codeLinesPerKTokens:
      tokenMeasuredTokens > 0 ? (tokenMeasuredCodeLines / tokenMeasuredTokens) * 1000 : null,
    tokensPerCodeLine:
      tokenMeasuredCodeLines > 0 ? tokenMeasuredTokens / tokenMeasuredCodeLines : null
  };
}

async function getRequirements(filters: FilterOptions) {
  const { fromSql, whereSql, params } = buildRoundSource(filters);
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       r.requirement_id AS requirementId,
       req.title AS title,
       req.project_name AS projectName,
       req.gpm_number AS gpmNumber,
       req.status AS requirementStatus,
       MIN(r.started_at) AS firstStartedAt,
       MAX(r.ended_at) AS lastEndedAt,
       COUNT(*) AS roundCount,
       COUNT(DISTINCT r.model_name) AS modelCount,
       GROUP_CONCAT(DISTINCT r.model_name ORDER BY r.model_name SEPARATOR ', ') AS models,
       COALESCE(SUM(r.total_tokens), 0) AS totalTokens,
       COALESCE(SUM(r.code_lines_changed), 0) AS codeLinesChanged,
       COALESCE(SUM(r.duration_ms), 0) AS durationMs,
       SUM(CASE WHEN r.total_tokens = 0 THEN 1 ELSE 0 END) AS tokenMissingRounds,
       COALESCE(SUM(CASE WHEN r.total_tokens > 0 THEN r.code_lines_changed ELSE 0 END), 0) AS tokenMeasuredCodeLines,
       COALESCE(SUM(CASE WHEN r.total_tokens > 0 THEN r.total_tokens ELSE 0 END), 0) AS tokenMeasuredTokens
     ${fromSql}
     LEFT JOIN ai_coding_requirements req ON req.requirement_id = r.requirement_id
     ${whereSql}
     GROUP BY r.requirement_id, req.title, req.project_name, req.gpm_number, req.status
     ORDER BY codeLinesChanged DESC, totalTokens DESC
     LIMIT 100`,
    params
  );

  const requirementId = (row: RowDataPacket) => row.requirementId === null ? null : Number(row.requirementId);
  return rows.map((row) => withEfficiency({
    requirementId: requirementId(row),
    requirementLabel: requirementLabel(requirementId(row), row.title),
    title: row.title ?? "",
    projectName: row.projectName ?? "",
    gpmNumber: row.gpmNumber ?? "",
    requirementStatus: row.requirementStatus ?? "",
    firstStartedAt: row.firstStartedAt ? toIso(row.firstStartedAt) : null,
    lastEndedAt: row.lastEndedAt ? toIso(row.lastEndedAt) : null,
    roundCount: Number(row.roundCount ?? 0),
    modelCount: Number(row.modelCount ?? 0),
    models: row.models ?? "",
    totalTokens: Number(row.totalTokens ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    durationMs: Number(row.durationMs ?? 0),
    tokenMissingRounds: Number(row.tokenMissingRounds ?? 0),
    tokenMeasuredTokens: Number(row.tokenMeasuredTokens ?? 0),
    tokenMeasuredCodeLines: Number(row.tokenMeasuredCodeLines ?? 0)
  }));
}

async function getModels(filters: FilterOptions) {
  const { whereSql, params } = buildWhere(filters, "r");
  const includeAll = filters.includeReverted;
  const activeCase = includeAll ? "1 = 1" : "rr.id IS NULL";
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       r.model_name AS modelName,
       COUNT(*) AS allRounds,
       SUM(CASE WHEN rr.id IS NULL THEN 1 ELSE 0 END) AS effectiveRounds,
       SUM(CASE WHEN rr.id IS NOT NULL THEN 1 ELSE 0 END) AS revertedRounds,
       COALESCE(SUM(CASE WHEN ${activeCase} THEN r.total_tokens ELSE 0 END), 0) AS totalTokens,
       COALESCE(SUM(CASE WHEN ${activeCase} THEN r.code_lines_changed ELSE 0 END), 0) AS codeLinesChanged,
       COALESCE(SUM(CASE WHEN ${activeCase} THEN r.duration_ms ELSE 0 END), 0) AS durationMs,
       SUM(CASE WHEN ${activeCase} AND r.total_tokens = 0 THEN 1 ELSE 0 END) AS tokenMissingRounds,
       COALESCE(SUM(CASE WHEN ${activeCase} AND r.total_tokens > 0 THEN r.code_lines_changed ELSE 0 END), 0) AS tokenMeasuredCodeLines,
       COALESCE(SUM(CASE WHEN ${activeCase} AND r.total_tokens > 0 THEN r.total_tokens ELSE 0 END), 0) AS tokenMeasuredTokens
     FROM ai_coding_rounds r
     LEFT JOIN ai_coding_round_reverts rr ON rr.target_round_id = r.id
     ${whereSql}
     GROUP BY r.model_name
     ORDER BY codeLinesChanged DESC, totalTokens DESC`,
    params
  );

  return rows.map((row) => {
    const allRounds = Number(row.allRounds ?? 0);
    const effectiveRounds = Number(row.effectiveRounds ?? 0);

    return withEfficiency({
      modelName: row.modelName,
      allRounds,
      effectiveRounds,
      revertedRounds: Number(row.revertedRounds ?? 0),
      revertRate: allRounds > 0 ? Number(row.revertedRounds ?? 0) / allRounds : 0,
      averageDurationMs: effectiveRounds > 0 ? Number(row.durationMs ?? 0) / effectiveRounds : 0,
      totalTokens: Number(row.totalTokens ?? 0),
      codeLinesChanged: Number(row.codeLinesChanged ?? 0),
      durationMs: Number(row.durationMs ?? 0),
      tokenMissingRounds: Number(row.tokenMissingRounds ?? 0),
      tokenMeasuredTokens: Number(row.tokenMeasuredTokens ?? 0),
      tokenMeasuredCodeLines: Number(row.tokenMeasuredCodeLines ?? 0)
    });
  });
}

async function getTimeline(filters: FilterOptions) {
  const { fromSql, whereSql, params } = buildRoundSource(filters);
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       DATE_FORMAT(started_at, '%Y-%m-%d') AS day,
       COUNT(*) AS roundCount,
       COUNT(DISTINCT requirement_id) AS requirementCount,
       COALESCE(SUM(total_tokens), 0) AS totalTokens,
       COALESCE(SUM(code_lines_changed), 0) AS codeLinesChanged,
       COALESCE(SUM(duration_ms), 0) AS durationMs,
       COALESCE(SUM(CASE WHEN total_tokens > 0 THEN code_lines_changed ELSE 0 END), 0) AS tokenMeasuredCodeLines,
       COALESCE(SUM(CASE WHEN total_tokens > 0 THEN total_tokens ELSE 0 END), 0) AS tokenMeasuredTokens
     ${fromSql}
     ${whereSql}
     GROUP BY DATE_FORMAT(started_at, '%Y-%m-%d')
     ORDER BY day ASC
     LIMIT 180`,
    params
  );

  return rows.map((row) => withEfficiency({
    day: String(row.day),
    roundCount: Number(row.roundCount ?? 0),
    requirementCount: Number(row.requirementCount ?? 0),
    totalTokens: Number(row.totalTokens ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    durationMs: Number(row.durationMs ?? 0),
    tokenMeasuredTokens: Number(row.tokenMeasuredTokens ?? 0),
    tokenMeasuredCodeLines: Number(row.tokenMeasuredCodeLines ?? 0)
  }));
}

async function getRounds(filters: FilterOptions, limit: number) {
  const { fromSql, whereSql, params } = buildRoundSource(filters);
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       r.id,
       r.conversation_id AS conversationId,
       r.requirement_id AS requirementId,
       r.requirement_source AS requirementSource,
       req.title AS requirementTitle,
       req.project_name AS projectName,
       req.gpm_number AS gpmNumber,
       r.model_name AS modelName,
       r.started_at AS startedAt,
       r.ended_at AS endedAt,
       r.duration_ms AS durationMs,
       r.prompt_text AS promptText,
       r.files_changed AS filesChanged,
       r.lines_added AS linesAdded,
       r.lines_deleted AS linesDeleted,
       r.code_lines_changed AS codeLinesChanged,
       r.input_tokens AS inputTokens,
       r.output_tokens AS outputTokens,
       r.total_tokens AS totalTokens,
       r.token_source AS tokenSource,
       r.token_sync_status AS tokenSyncStatus,
       r.token_synced_at AS tokenSyncedAt,
       r.token_sync_note AS tokenSyncNote,
       JSON_UNQUOTE(JSON_EXTRACT(r.metadata, '$.client')) AS client,
       r.is_reverted AS isReverted
     ${fromSql}
     LEFT JOIN ai_coding_requirements req ON req.requirement_id = r.requirement_id
     ${whereSql}
     ORDER BY r.ended_at DESC, r.id DESC
     LIMIT ${limit}`,
    params
  );

  const requirementId = (row: RowDataPacket) => row.requirementId === null ? null : Number(row.requirementId);
  return rows.map((row) => ({
    id: Number(row.id),
    conversationId: row.conversationId,
    requirementId: requirementId(row),
    requirementSource: row.requirementSource,
    requirementLabel: requirementLabel(requirementId(row), row.requirementTitle),
    requirementTitle: row.requirementTitle ?? "",
    projectName: row.projectName ?? "",
    gpmNumber: row.gpmNumber ?? "",
    modelName: row.modelName,
    startedAt: toIso(row.startedAt),
    endedAt: toIso(row.endedAt),
    durationMs: Number(row.durationMs ?? 0),
    promptText: row.promptText ?? "",
    filesChanged: row.filesChanged === null ? null : Number(row.filesChanged),
    linesAdded: Number(row.linesAdded ?? 0),
    linesDeleted: Number(row.linesDeleted ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    inputTokens: Number(row.inputTokens ?? 0),
    outputTokens: Number(row.outputTokens ?? 0),
    totalTokens: Number(row.totalTokens ?? 0),
    tokenSource: row.tokenSource ?? "mcp_payload",
    tokenSyncStatus: row.tokenSyncStatus ?? "pending",
    tokenSyncedAt: row.tokenSyncedAt ? toIso(row.tokenSyncedAt) : null,
    tokenSyncNote: row.tokenSyncNote ?? "",
    client: row.client ?? "",
    isReverted: Number(row.isReverted ?? 0) === 1
  }));
}

async function getFilters() {
  const [models] = await getPool().execute<RowDataPacket[]>(
    `SELECT DISTINCT model_name AS value
     FROM ai_coding_rounds
     ORDER BY model_name`
  );
  const [requirements] = await getPool().execute<RowDataPacket[]>(
    `SELECT ids.requirement_id AS value,
            req.title,
            req.project_name AS projectName,
            req.gpm_number AS gpmNumber
     FROM (
       SELECT DISTINCT requirement_id
       FROM ai_coding_rounds
       WHERE requirement_id IS NOT NULL
       UNION
       SELECT requirement_id
       FROM ai_coding_requirements
     ) ids
     LEFT JOIN ai_coding_requirements req ON req.requirement_id = ids.requirement_id
     ORDER BY ids.requirement_id`
  );
  const [clients] = await getPool().execute<RowDataPacket[]>(
    `SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.client')) AS value
     FROM ai_coding_rounds
     WHERE JSON_EXTRACT(metadata, '$.client') IS NOT NULL
     ORDER BY value`
  );

  return {
    models: models.map((row) => row.value).filter(Boolean),
    requirements: [
      {
        id: null,
        title: "",
        projectName: "",
        gpmNumber: "",
        label: "未关联需求"
      },
      ...requirements.map((row) => {
      const id = Number(row.value);
      return {
        id,
        title: row.title ?? "",
        projectName: row.projectName ?? "",
        gpmNumber: row.gpmNumber ?? "",
        label: requirementLabel(id, row.title)
      };
    })],
    clients: clients.map((row) => row.value).filter(Boolean)
  };
}

async function getRequirementRecords() {
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       ids.requirement_id AS requirementId,
       req.title,
       req.project_name AS projectName,
       req.gpm_number AS gpmNumber,
       req.status,
       req.description,
       req.created_at AS createdAt,
       req.updated_at AS updatedAt,
       COUNT(r.id) AS roundCount,
       COALESCE(SUM(r.total_tokens), 0) AS totalTokens,
       COALESCE(SUM(r.code_lines_changed), 0) AS codeLinesChanged,
       COALESCE(SUM(r.duration_ms), 0) AS durationMs
     FROM (
       SELECT requirement_id
       FROM ai_coding_requirements
       UNION
       SELECT DISTINCT requirement_id
       FROM ai_coding_rounds
       WHERE requirement_id IS NOT NULL
     ) ids
     LEFT JOIN ai_coding_requirements req ON req.requirement_id = ids.requirement_id
     LEFT JOIN ai_coding_effective_rounds r ON r.requirement_id = ids.requirement_id
     GROUP BY
       ids.requirement_id,
       req.title,
       req.project_name,
       req.gpm_number,
       req.status,
       req.description,
       req.created_at,
       req.updated_at
     ORDER BY ids.requirement_id`
  );

  return rows.map((row) => ({
    requirementId: Number(row.requirementId),
    requirementLabel: requirementLabel(Number(row.requirementId), row.title),
    title: row.title ?? "",
    projectName: row.projectName ?? "",
    gpmNumber: row.gpmNumber ?? "",
    status: row.status ?? "active",
    description: row.description ?? "",
    createdAt: row.createdAt ? toIso(row.createdAt) : null,
    updatedAt: row.updatedAt ? toIso(row.updatedAt) : null,
    roundCount: Number(row.roundCount ?? 0),
    totalTokens: Number(row.totalTokens ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    durationMs: Number(row.durationMs ?? 0)
  }));
}

async function saveRequirementRecord(input: Record<string, unknown>) {
  const normalized = normalizeRequirementInput(input);
  await getPool().execute<ResultSetHeader>(
    `INSERT INTO ai_coding_requirements (
       requirement_id,
       title,
       project_name,
       gpm_number,
       status,
       description
     ) VALUES (
       :requirementId,
       :title,
       :projectName,
       :gpmNumber,
       :status,
       :description
     )
     ON DUPLICATE KEY UPDATE
       title = VALUES(title),
       project_name = VALUES(project_name),
       gpm_number = VALUES(gpm_number),
       status = VALUES(status),
       description = VALUES(description)`,
    normalized
  );

  const records = await getRequirementRecords();
  return records.find((record) => record.requirementId === normalized.requirementId);
}

async function deleteRequirementRecord(requirementId: number) {
  if (!Number.isSafeInteger(requirementId) || requirementId <= 0) {
    throw new Error("requirementId must be a positive integer");
  }

  const [result] = await getPool().execute<ResultSetHeader>(
    `DELETE FROM ai_coding_requirements
     WHERE requirement_id = :requirementId`,
    { requirementId }
  );

  return {
    ok: true,
    requirementId,
    deleted: result.affectedRows > 0
  };
}

async function updateRoundRecord(roundId: number, input: Record<string, unknown>) {
  const normalized = normalizeRoundInput(input);
  const connection = await getPool().getConnection();
  try {
    await connection.beginTransaction();

    const [existingRows] = await connection.execute<RowDataPacket[]>(
      `SELECT conversation_id, metadata
       FROM ai_coding_rounds
       WHERE id = :roundId
       FOR UPDATE`,
      { roundId }
    );
    const existing = existingRows[0];
    if (!existing) {
      throw new Error(`Round ${roundId} not found`);
    }

    const metadata = parseMetadataValue(existing.metadata);
    if (normalized.client) {
      metadata.client = normalized.client;
    } else {
      delete metadata.client;
    }

    await connection.execute<ResultSetHeader>(
      `UPDATE ai_coding_rounds
       SET requirement_id = :requirementId,
           requirement_source = :requirementSource,
           model_name = :modelName,
           started_at = :startedAt,
           ended_at = :endedAt,
           prompt_text = :promptText,
           files_changed = :filesChanged,
           lines_added = :linesAdded,
           lines_deleted = :linesDeleted,
           code_lines_changed = :codeLinesChanged,
           input_tokens = :inputTokens,
           output_tokens = :outputTokens,
           total_tokens = :totalTokens,
           metadata = CAST(:metadata AS JSON)
       WHERE id = :roundId`,
      {
        roundId,
        requirementId: normalized.requirementId,
        requirementSource: normalized.requirementSource,
        modelName: normalized.modelName,
        startedAt: toMysqlDateTime(normalized.startedAt),
        endedAt: toMysqlDateTime(normalized.endedAt),
        promptText: normalized.promptText,
        filesChanged: normalized.filesChanged,
        linesAdded: normalized.linesAdded,
        linesDeleted: normalized.linesDeleted,
        codeLinesChanged: normalized.codeLinesChanged,
        inputTokens: normalized.inputTokens,
        outputTokens: normalized.outputTokens,
        totalTokens: normalized.totalTokens,
        metadata: JSON.stringify(Object.keys(metadata).length > 0 ? metadata : null)
      }
    );

    await refreshConversationContext(connection, String(existing.conversation_id));
    await connection.commit();
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }

  const rounds = await getRoundById(roundId);
  return rounds[0];
}

async function deleteRoundRecord(roundId: number) {
  const connection = await getPool().getConnection();
  try {
    await connection.beginTransaction();

    const [existingRows] = await connection.execute<RowDataPacket[]>(
      `SELECT conversation_id
       FROM ai_coding_rounds
       WHERE id = :roundId
       FOR UPDATE`,
      { roundId }
    );
    const existing = existingRows[0];
    if (!existing) {
      await connection.rollback();
      return { ok: true, roundId, deleted: false };
    }

    await connection.execute<ResultSetHeader>(
      `DELETE FROM ai_coding_rounds
       WHERE id = :roundId`,
      { roundId }
    );
    await refreshConversationContext(connection, String(existing.conversation_id));
    await connection.commit();

    return { ok: true, roundId, deleted: true };
  } catch (error) {
    await connection.rollback();
    throw error;
  } finally {
    connection.release();
  }
}

async function getRoundById(roundId: number) {
  const [rows] = await getPool().execute<RowDataPacket[]>(
    `SELECT
       r.id,
       r.conversation_id AS conversationId,
       r.requirement_id AS requirementId,
       r.requirement_source AS requirementSource,
       req.title AS requirementTitle,
       req.project_name AS projectName,
       req.gpm_number AS gpmNumber,
       r.model_name AS modelName,
       r.started_at AS startedAt,
       r.ended_at AS endedAt,
       r.duration_ms AS durationMs,
       r.prompt_text AS promptText,
       r.files_changed AS filesChanged,
       r.lines_added AS linesAdded,
       r.lines_deleted AS linesDeleted,
       r.code_lines_changed AS codeLinesChanged,
       r.input_tokens AS inputTokens,
       r.output_tokens AS outputTokens,
       r.total_tokens AS totalTokens,
       r.token_source AS tokenSource,
       r.token_sync_status AS tokenSyncStatus,
       r.token_synced_at AS tokenSyncedAt,
       r.token_sync_note AS tokenSyncNote,
       JSON_UNQUOTE(JSON_EXTRACT(r.metadata, '$.client')) AS client,
       CASE WHEN rr.id IS NULL THEN 0 ELSE 1 END AS isReverted
     FROM ai_coding_rounds r
     LEFT JOIN ai_coding_round_reverts rr ON rr.target_round_id = r.id
     LEFT JOIN ai_coding_requirements req ON req.requirement_id = r.requirement_id
     WHERE r.id = :roundId`,
    { roundId }
  );

  const requirementId = (row: RowDataPacket) => row.requirementId === null ? null : Number(row.requirementId);
  return rows.map((row) => ({
    id: Number(row.id),
    conversationId: row.conversationId,
    requirementId: requirementId(row),
    requirementSource: row.requirementSource,
    requirementLabel: requirementLabel(requirementId(row), row.requirementTitle),
    requirementTitle: row.requirementTitle ?? "",
    projectName: row.projectName ?? "",
    gpmNumber: row.gpmNumber ?? "",
    modelName: row.modelName,
    startedAt: toIso(row.startedAt),
    endedAt: toIso(row.endedAt),
    durationMs: Number(row.durationMs ?? 0),
    promptText: row.promptText ?? "",
    filesChanged: row.filesChanged === null ? null : Number(row.filesChanged),
    linesAdded: Number(row.linesAdded ?? 0),
    linesDeleted: Number(row.linesDeleted ?? 0),
    codeLinesChanged: Number(row.codeLinesChanged ?? 0),
    inputTokens: Number(row.inputTokens ?? 0),
    outputTokens: Number(row.outputTokens ?? 0),
    totalTokens: Number(row.totalTokens ?? 0),
    tokenSource: row.tokenSource ?? "mcp_payload",
    tokenSyncStatus: row.tokenSyncStatus ?? "pending",
    tokenSyncedAt: row.tokenSyncedAt ? toIso(row.tokenSyncedAt) : null,
    tokenSyncNote: row.tokenSyncNote ?? "",
    client: row.client ?? "",
    isReverted: Number(row.isReverted ?? 0) === 1
  }));
}

async function refreshConversationContext(
  connection: PoolConnection,
  conversationId: string
) {
  const [rows] = await connection.execute<RowDataPacket[]>(
    `SELECT id, requirement_id
     FROM ai_coding_rounds
     WHERE conversation_id = :conversationId
     ORDER BY ended_at DESC, id DESC
     LIMIT 1`,
    { conversationId }
  );
  const latest = rows[0];

  await connection.execute(
    `UPDATE ai_coding_conversations
     SET current_requirement_id = :requirementId,
         last_round_id = :lastRoundId
     WHERE conversation_id = :conversationId`,
    {
      conversationId,
      requirementId: latest?.requirement_id ?? null,
      lastRoundId: latest?.id ?? null
    }
  );
}

async function getLocalLogFiles(searchParams: URLSearchParams) {
  const client = normalizeLogClient(searchParams.get("client") ?? "codex");
  const search = (searchParams.get("search") ?? "").trim().toLowerCase();
  const limit = Math.min(
    Math.max(Number(searchParams.get("limit") ?? 50), 1),
    maxLogFilesReturned
  );

  const files = await discoverLocalLogFiles(client, search);
  return {
    client,
    limit,
    scanned: files.scanned,
    truncated: files.truncated,
    files: files.items.slice(0, limit)
  };
}

async function getLocalLogFile(searchParams: URLSearchParams) {
  const requestedPath = searchParams.get("path");
  if (!requestedPath) {
    throw new Error("path is required");
  }
  const filePath = assertAllowedLogPath(requestedPath);
  const fileStat = await stat(filePath);
  if (!fileStat.isFile()) {
    throw new Error("path must point to a file");
  }

  const tailBytes = Math.min(
    Math.max(Number(searchParams.get("tailBytes") ?? defaultLogTailBytes), 1024),
    maxLogTailBytes
  );
  const ext = path.extname(filePath);
  if (ext === ".sqlite" || ext === ".db") {
    return {
      path: filePath,
      name: path.basename(filePath),
      size: fileStat.size,
      mtime: fileStat.mtime.toISOString(),
      tailBytes: 0,
      truncated: false,
      binary: true,
      content: "",
      lines: []
    };
  }

  const start = Math.max(fileStat.size - tailBytes, 0);
  const length = fileStat.size - start;
  const buffer = Buffer.alloc(length);
  const handle = await open(filePath, "r");
  try {
    await handle.read(buffer, 0, length, start);
  } finally {
    await handle.close();
  }

  let content = buffer.toString("utf8");
  if (start > 0) {
    const firstLineBreak = content.indexOf("\n");
    if (firstLineBreak >= 0) {
      content = content.slice(firstLineBreak + 1);
    }
  }

  const rawLines = content.split(/\r?\n/).filter((line) => line.trim()).slice(-200);
  const lines = rawLines.map((line) => {
    const parsed = parseJsonObject(line);
    return {
      timestamp: stringValue(parsed?.timestamp) ?? stringValue(parsed?.created_at) ?? "",
      type: stringValue(parsed?.type) ?? stringValue(parsed?.event) ?? "",
      text: line.slice(0, 1200)
    };
  });

  return {
    path: filePath,
    name: path.basename(filePath),
    size: fileStat.size,
    mtime: fileStat.mtime.toISOString(),
    tailBytes: length,
    truncated: start > 0,
    binary: false,
    content,
    lines
  };
}

async function discoverLocalLogFiles(client: "codex" | "claude-code", search: string) {
  const roots = localLogRoots(client);
  const discovered: string[] = [];
  let truncated = false;

  for (const root of roots.directories) {
    const result = await walkLogFiles(root, ".jsonl", maxLogFilesScanned - discovered.length);
    discovered.push(...result.files);
    truncated = truncated || result.truncated;
    if (discovered.length >= maxLogFilesScanned) {
      truncated = true;
      break;
    }
  }

  if (client === "codex") {
    discovered.push(...roots.files);
  }

  const filtered = search
    ? discovered.filter((file) => file.toLowerCase().includes(search))
    : discovered;
  const items = (await Promise.all(filtered.map(async (file) => {
    const fileStat = await stat(file).catch(() => null);
    if (!fileStat?.isFile()) return null;
    return {
      path: file,
      name: path.basename(file),
      directory: path.dirname(file),
      size: fileStat.size,
      mtime: fileStat.mtime.toISOString(),
      kind: path.extname(file) === ".jsonl" ? "jsonl" : "sqlite"
    };
  })))
    .filter((item): item is NonNullable<typeof item> => item !== null)
    .sort((left, right) => new Date(right.mtime).getTime() - new Date(left.mtime).getTime());

  return {
    scanned: discovered.length,
    truncated,
    items
  };
}

async function walkLogFiles(root: string, suffix: string, remaining: number): Promise<{
  files: string[];
  truncated: boolean;
}> {
  if (remaining <= 0) return { files: [], truncated: true };
  const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
  const files: string[] = [];
  let truncated = false;

  for (const entry of entries) {
    if (files.length >= remaining) {
      truncated = true;
      break;
    }

    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      const child = await walkLogFiles(fullPath, suffix, remaining - files.length);
      files.push(...child.files);
      truncated = truncated || child.truncated;
    } else if (entry.isFile() && entry.name.endsWith(suffix)) {
      files.push(fullPath);
    }
  }

  return { files, truncated };
}

function localLogRoots(client: "codex" | "claude-code") {
  const home = homedir();
  if (client === "codex") {
    return {
      directories: [
        path.join(home, ".codex", "sessions"),
        path.join(home, ".codex", "archived_sessions")
      ],
      files: [
        path.join(home, ".codex", "logs_2.sqlite"),
        path.join(home, ".codex", "state_5.sqlite")
      ]
    };
  }

  return {
    directories: [path.join(home, ".claude", "projects")],
    files: []
  };
}

function assertAllowedLogPath(value: string): string {
  const resolved = path.resolve(value);
  const allRoots = [
    ...localLogRoots("codex").directories,
    ...localLogRoots("claude-code").directories,
    ...localLogRoots("codex").files
  ].map((entry) => path.resolve(entry));

  const allowed = allRoots.some((root) => (
    resolved === root || resolved.startsWith(`${root}${path.sep}`)
  ));
  if (!allowed) {
    throw new Error("path is outside supported local log roots");
  }
  return resolved;
}

function normalizeLogClient(value: string): "codex" | "claude-code" {
  if (value === "codex" || value === "claude-code") return value;
  throw new Error("client must be codex or claude-code");
}

function normalizeRequirementInput(input: Record<string, unknown>): RequirementRecordInput {
  const requirementId = Number(input.requirementId);
  if (!Number.isSafeInteger(requirementId) || requirementId <= 0) {
    throw new Error("requirementId must be a positive integer");
  }

  const status = optionalString(input.status) ?? "active";
  if (!["active", "done", "archived"].includes(status)) {
    throw new Error("status must be active, done, or archived");
  }

  return {
    requirementId,
    title: optionalString(input.title, 255),
    projectName: optionalString(input.projectName, 255),
    gpmNumber: optionalString(input.gpmNumber, 128),
    status,
    description: optionalString(input.description, 5000)
  };
}

function normalizeRoundInput(input: Record<string, unknown>): RoundRecordInput {
  const requirementId = nullablePositiveInteger(input.requirementId, "requirementId");
  const modelName = optionalString(input.modelName, 128);
  if (!modelName) {
    throw new Error("modelName is required");
  }

  const startedAt = requiredDateString(input.startedAt, "startedAt");
  const endedAt = requiredDateString(input.endedAt, "endedAt");
  if (new Date(endedAt).getTime() < new Date(startedAt).getTime()) {
    throw new Error("endedAt must be greater than or equal to startedAt");
  }

  const linesAdded = nonNegativeInteger(input.linesAdded, "linesAdded");
  const linesDeleted = nonNegativeInteger(input.linesDeleted, "linesDeleted");
  const inputTokens = nonNegativeInteger(input.inputTokens, "inputTokens");
  const outputTokens = nonNegativeInteger(input.outputTokens, "outputTokens");
  const totalTokens = input.totalTokens === undefined || input.totalTokens === null || input.totalTokens === ""
    ? inputTokens + outputTokens
    : nonNegativeInteger(input.totalTokens, "totalTokens");
  const source = optionalString(input.requirementSource, 16);
  const requirementSource = requirementId === null
    ? "empty"
    : source === "context" || source === "prompt" ? source : "prompt";

  return {
    requirementId,
    requirementSource,
    modelName,
    startedAt,
    endedAt,
    promptText: optionalString(input.promptText, 20_000),
    filesChanged: nullableNonNegativeInteger(input.filesChanged, "filesChanged"),
    linesAdded,
    linesDeleted,
    codeLinesChanged: input.codeLinesChanged === undefined || input.codeLinesChanged === null || input.codeLinesChanged === ""
      ? linesAdded + linesDeleted
      : nonNegativeInteger(input.codeLinesChanged, "codeLinesChanged"),
    inputTokens,
    outputTokens,
    totalTokens,
    client: optionalString(input.client, 64)
  };
}

function optionalString(value: unknown, maxLength = 512): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") {
    throw new Error("Requirement fields must be strings");
  }

  const trimmed = value.trim();
  if (!trimmed) return null;
  return trimmed.slice(0, maxLength);
}

function parseMetadataValue(value: unknown): Record<string, unknown> {
  if (!value) return {};
  if (typeof value === "object" && !Array.isArray(value)) {
    return { ...(value as Record<string, unknown>) };
  }
  if (typeof value === "string") {
    return parseJsonObject(value) ?? {};
  }
  return {};
}

function parseJsonObject(value: string): Record<string, unknown> | null {
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
      ? parsed as Record<string, unknown>
      : null;
  } catch {
    return null;
  }
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function nonNegativeInteger(value: unknown, fieldName: string): number {
  const number = Number(value ?? 0);
  if (!Number.isSafeInteger(number) || number < 0) {
    throw new Error(`${fieldName} must be a non-negative integer`);
  }
  return number;
}

function nullableNonNegativeInteger(value: unknown, fieldName: string): number | null {
  if (value === undefined || value === null || value === "") return null;
  return nonNegativeInteger(value, fieldName);
}

function nullablePositiveInteger(value: unknown, fieldName: string): number | null {
  if (value === undefined || value === null || value === "") return null;
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) {
    throw new Error(`${fieldName} must be a positive integer or empty`);
  }
  return number;
}

function requiredDateString(value: unknown, fieldName: string): string {
  if (typeof value !== "string" || !value.trim()) {
    throw new Error(`${fieldName} is required`);
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`${fieldName} must be a valid date-time string`);
  }
  return date.toISOString();
}

function requirementLabel(requirementId: number | null, title?: unknown): string {
  if (requirementId === null) return "未关联需求";
  const normalizedTitle = typeof title === "string" ? title.trim() : "";
  return normalizedTitle ? `#${requirementId} ${normalizedTitle}` : `#${requirementId}`;
}

function buildRoundSource(filters: FilterOptions) {
  const source = filters.includeReverted
    ? `FROM (
         SELECT r.*, CASE WHEN rr.id IS NULL THEN 0 ELSE 1 END AS is_reverted
         FROM ai_coding_rounds r
         LEFT JOIN ai_coding_round_reverts rr ON rr.target_round_id = r.id
       ) r`
    : `FROM (
         SELECT r.*, 0 AS is_reverted
         FROM ai_coding_effective_rounds r
       ) r`;
  const { whereSql, params } = buildWhere(filters, "r");

  return {
    fromSql: source,
    whereSql,
    params
  };
}

function buildWhere(filters: FilterOptions, alias: string) {
  const conditions: string[] = [];
  const params: QueryParams = {};

  if (filters.from) {
    conditions.push(`${alias}.started_at >= :from`);
    params.from = toMysqlDateTime(filters.from);
  }

  if (filters.to) {
    conditions.push(`${alias}.started_at <= :to`);
    params.to = toMysqlDateTime(filters.to, true);
  }

  if (filters.model) {
    conditions.push(`${alias}.model_name = :model`);
    params.model = filters.model;
  }

  if (filters.requirementId) {
    if (filters.requirementId === "null") {
      conditions.push(`${alias}.requirement_id IS NULL`);
    } else {
      conditions.push(`${alias}.requirement_id = :requirementId`);
      params.requirementId = Number(filters.requirementId);
    }
  }

  if (filters.client) {
    conditions.push(`JSON_UNQUOTE(JSON_EXTRACT(${alias}.metadata, '$.client')) = :client`);
    params.client = filters.client;
  }

  return {
    whereSql: conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "",
    params
  };
}

function parseFilters(searchParams: URLSearchParams): FilterOptions {
  const requirementId = searchParams.get("requirementId") ?? undefined;
  if (requirementId && requirementId !== "null" && !Number.isSafeInteger(Number(requirementId))) {
    throw new Error("requirementId must be a number or null");
  }

  return {
    from: searchParams.get("from") ?? undefined,
    to: searchParams.get("to") ?? undefined,
    model: searchParams.get("model") ?? undefined,
    requirementId,
    client: searchParams.get("client") ?? undefined,
    includeReverted: searchParams.get("includeReverted") === "true"
  };
}

function withEfficiency<T extends {
  tokenMeasuredTokens: number;
  tokenMeasuredCodeLines: number;
}>(row: T) {
  return {
    ...row,
    codeLinesPerKTokens:
      row.tokenMeasuredTokens > 0 ? (row.tokenMeasuredCodeLines / row.tokenMeasuredTokens) * 1000 : null,
    tokensPerCodeLine:
      row.tokenMeasuredCodeLines > 0 ? row.tokenMeasuredTokens / row.tokenMeasuredCodeLines : null
  };
}

async function handleLogin(
  request: IncomingMessage,
  response: ServerResponse,
  config: DashboardConfig
): Promise<void> {
  const body = await readJsonBody(request);
  const username = typeof body.username === "string" ? body.username : "";
  const password = typeof body.password === "string" ? body.password : "";

  if (!secureEqual(username, config.username) || !secureEqual(password, config.password)) {
    sendJson(response, 401, { error: "invalid_credentials" });
    return;
  }

  setSessionCookie(response, username, config);
  sendJson(response, 200, { ok: true });
}

function setSessionCookie(response: ServerResponse, username: string, config: DashboardConfig): void {
  const payload = Buffer.from(JSON.stringify({
    username,
    expiresAt: Date.now() + config.sessionTtlMs
  })).toString("base64url");
  const signature = sign(payload, config.sessionSecret);

  response.setHeader(
    "Set-Cookie",
    `${cookieName}=${payload}.${signature}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${Math.floor(config.sessionTtlMs / 1000)}`
  );
}

function clearSessionCookie(response: ServerResponse): void {
  response.setHeader("Set-Cookie", `${cookieName}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`);
}

function isAuthenticated(request: IncomingMessage, config: DashboardConfig): boolean {
  const cookies = parseCookies(request.headers.cookie ?? "");
  const session = cookies[cookieName];
  if (!session) return false;

  const [payload, signature] = session.split(".");
  if (!payload || !signature || !secureEqual(signature, sign(payload, config.sessionSecret))) {
    return false;
  }

  try {
    const parsed = JSON.parse(Buffer.from(payload, "base64url").toString("utf8"));
    return parsed.username === config.username && Number(parsed.expiresAt) > Date.now();
  } catch {
    return false;
  }
}

function parseCookies(cookieHeader: string): Record<string, string> {
  const cookies: Record<string, string> = {};
  for (const part of cookieHeader.split(";")) {
    const [key, ...valueParts] = part.trim().split("=");
    if (!key) continue;
    cookies[key] = decodeURIComponent(valueParts.join("="));
  }
  return cookies;
}

function sign(payload: string, secret: string): string {
  return createHmac("sha256", secret).update(payload).digest("base64url");
}

function secureEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  if (leftBuffer.length !== rightBuffer.length) return false;

  return timingSafeEqual(leftBuffer, rightBuffer);
}

async function readJsonBody(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }

  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

async function sendStatic(response: ServerResponse, relativePath: string): Promise<void> {
  const safePath = path.normalize(relativePath).replace(/^(\.\.[/\\])+/, "");
  const filePath = path.join(staticRoot, safePath);
  if (!filePath.startsWith(staticRoot)) {
    sendJson(response, 403, { error: "forbidden" });
    return;
  }

  try {
    const fileStat = await stat(filePath);
    if (!fileStat.isFile()) {
      sendJson(response, 404, { error: "not_found" });
      return;
    }

    response.writeHead(200, {
      "Content-Type": mimeTypes[path.extname(filePath)] ?? "application/octet-stream",
      "Cache-Control": path.extname(filePath) === ".html" ? "no-store" : "public, max-age=3600"
    });
    createReadStream(filePath).pipe(response);
  } catch {
    sendJson(response, 404, { error: "not_found" });
  }
}

function sendJson(response: ServerResponse, statusCode: number, payload: unknown): void {
  response.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store"
  });
  response.end(JSON.stringify(payload));
}

function redirect(response: ServerResponse, location: string): void {
  response.writeHead(302, { Location: location });
  response.end();
}

function toMysqlDateTime(value: string, endOfDay = false): string {
  const normalizedValue = endOfDay && /^\d{4}-\d{2}-\d{2}$/.test(value)
    ? `${value}T23:59:59.999Z`
    : value;
  const date = new Date(normalizedValue);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`Invalid date: ${value}`);
  }

  return date.toISOString().slice(0, 23).replace("T", " ");
}

function toIso(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  return new Date(String(value)).toISOString();
}

const isMain = process.argv[1] && pathToFileURL(process.argv[1]).href === import.meta.url;

if (isMain) {
  const config = getDashboardConfig();
  const server = createDashboardServer(config);
  server.listen(config.port, config.host, () => {
    console.log(`AI Coding Stats dashboard listening on http://${config.host}:${config.port}`);
  });
}

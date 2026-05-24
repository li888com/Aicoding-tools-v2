import mysql, { Pool } from "mysql2/promise";
import { getAiCodingApiConfig, getDatabaseConfig } from "./config.js";

export type RecordRoundInput = {
  conversationId: string;
  startedAt: string;
  endedAt: string;
  modelName: string;
  promptText?: string;
  filesChanged?: number;
  linesAdded?: number;
  linesDeleted?: number;
  codeLinesChanged?: number;
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  metadata?: Record<string, unknown>;
};

export type RecordedRound = {
  id: number;
  conversationId: string;
  requirementId: number | string | null;
  requirementSource: string;
  modelName: string;
  durationMs: number;
  codeLinesChanged: number;
  totalTokens: number;
};

export type RecordRoundRevertInput = {
  conversationId: string;
  targetRoundId?: number;
  revertedAt: string;
  modelName: string;
  promptText?: string;
  reason?: string;
  filesChanged?: number;
  linesAdded?: number;
  linesDeleted?: number;
  codeLinesChanged?: number;
  inputTokens?: number;
  outputTokens?: number;
  totalTokens?: number;
  metadata?: Record<string, unknown>;
};

export type RecordedRoundRevert = {
  id: number;
  targetRoundId: number;
  conversationId: string;
  requirementId: number | string | null;
  modelName: string;
  revertedAt: string;
  codeLinesChanged: number;
  totalTokens: number;
};

export type DemandListItem = {
  demandId: string;
  demandCode: string;
  demandName: string;
  phaseName?: string | null;
  projectCode?: string | null;
  projectName?: string | null;
};

export type DemandListResponse = {
  code: number;
  data: DemandListItem[];
  msg?: string;
};

export type DemandBindInput = {
  conversationId: string;
  demandId: string;
  demandCode?: string;
  demandName?: string;
  userId?: string;
  client?: string;
};

export type DemandBindResponse = {
  conversationId: string;
  userId: string;
  demandId: string;
  demandCode?: string;
  demandName?: string;
  client?: string;
  boundAt?: string;
};

let pool: Pool | null = null;

export function getPool(): Pool {
  if (!pool) {
    const config = getDatabaseConfig();
    pool = mysql.createPool({
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
      database: config.database,
      connectionLimit: config.connectionLimit,
      timezone: "Z",
      namedPlaceholders: true,
      decimalNumbers: true
    });
  }

  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}

export async function recordRound(input: RecordRoundInput): Promise<RecordedRound> {
  validateInput(input);
  return postJson<RecordedRound>(getAiCodingApiConfig().roundRecordPath, withConfiguredUser(input));
}

export async function recordRoundRevert(input: RecordRoundRevertInput): Promise<RecordedRoundRevert> {
  validateRevertInput(input);
  return postJson<RecordedRoundRevert>(getAiCodingApiConfig().roundRevertPath, withConfiguredUser(input));
}

export async function listDemands(userId?: string): Promise<DemandListResponse> {
  return postJson<DemandListResponse>(getAiCodingApiConfig().demandListPath, {
    userId: userId?.trim() || getAiCodingApiConfig().userId
  });
}

export async function bindDemand(input: DemandBindInput): Promise<DemandBindResponse> {
  if (!input.conversationId.trim()) {
    throw new Error("conversationId is required");
  }

  if (!input.demandId.trim()) {
    throw new Error("demandId is required");
  }

  return postJson<DemandBindResponse>(getAiCodingApiConfig().demandBindPath, {
    ...input,
    userId: input.userId?.trim() || getAiCodingApiConfig().userId
  });
}

function withConfiguredUser<T extends { metadata?: Record<string, unknown> }>(input: T): T & { userId: string } {
  const config = getAiCodingApiConfig();
  return {
    ...input,
    userId: config.userId
  };
}

function validateInput(input: RecordRoundInput): void {
  if (!input.conversationId.trim()) {
    throw new Error("conversationId is required");
  }

  if (!input.modelName.trim()) {
    throw new Error("modelName is required");
  }

  const startedAt = new Date(input.startedAt);
  const endedAt = new Date(input.endedAt);
  if (Number.isNaN(startedAt.getTime())) {
    throw new Error("startedAt must be a valid date-time string");
  }

  if (Number.isNaN(endedAt.getTime())) {
    throw new Error("endedAt must be a valid date-time string");
  }

  if (endedAt.getTime() < startedAt.getTime()) {
    throw new Error("endedAt must be greater than or equal to startedAt");
  }
}

function validateRevertInput(input: RecordRoundRevertInput): void {
  if (!input.conversationId.trim()) {
    throw new Error("conversationId is required");
  }

  if (!input.modelName.trim()) {
    throw new Error("modelName is required");
  }

  if (input.targetRoundId !== undefined && (!Number.isSafeInteger(input.targetRoundId) || input.targetRoundId <= 0)) {
    throw new Error("targetRoundId must be a positive integer");
  }

  const revertedAt = new Date(input.revertedAt);
  if (Number.isNaN(revertedAt.getTime())) {
    throw new Error("revertedAt must be a valid date-time string");
  }
}

async function postJson<T>(path: string, payload: unknown): Promise<T> {
  const config = getAiCodingApiConfig();
  const response = await fetch(`${config.baseUrl}${normalizePath(path)}`, {
    method: "POST",
    headers: {
      "content-type": "application/json"
    },
    body: JSON.stringify(payload)
  });

  const text = await response.text();
  const data = text ? safeJsonParse(text) : null;

  if (!response.ok) {
    throw new Error(`AI Coding backend request failed with ${response.status}: ${text || response.statusText}`);
  }

  if (data && typeof data === "object" && "data" in data && "code" in data) {
    const wrapper = data as { code?: number; data?: T; msg?: string };
    if (wrapper.code !== undefined && wrapper.code !== 200 && wrapper.code !== 0) {
      throw new Error(wrapper.msg || `AI Coding backend returned code ${wrapper.code}`);
    }

    return (wrapper.data as T | undefined) ?? (data as T);
  }

  return data as T;
}

function normalizePath(path: string): string {
  return path.startsWith("/") ? path : `/${path}`;
}

function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

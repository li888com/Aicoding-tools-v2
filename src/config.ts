import { config as loadDotenv } from "dotenv";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const moduleDir = dirname(fileURLToPath(import.meta.url));
loadDotenv();
loadDotenv({ path: resolve(moduleDir, "../.env") });

export type DatabaseConfig = {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  connectionLimit: number;
};

export type FeishuConfig = {
  appId: string;
  appSecret: string;
  baseUrl: string;
};

function readNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (!value) return fallback;

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
}

export function getDatabaseConfig(): DatabaseConfig {
  return {
    host: process.env.MYSQL_HOST ?? "127.0.0.1",
    port: readNumber("MYSQL_PORT", 3306),
    user: process.env.MYSQL_USER ?? "ai_coding",
    password: process.env.MYSQL_PASSWORD ?? "ai_coding_pass",
    database: process.env.MYSQL_DATABASE ?? "ai_coding_stats",
    connectionLimit: readNumber("MYSQL_CONNECTION_LIMIT", 10)
  };
}

export function getFeishuConfig(): FeishuConfig {
  const appId = process.env.FEISHU_APP_ID?.trim();
  const appSecret = process.env.FEISHU_APP_SECRET?.trim();

  if (!appId || !appSecret) {
    throw new Error("FEISHU_APP_ID and FEISHU_APP_SECRET are required for Feishu document tools");
  }

  return {
    appId,
    appSecret,
    baseUrl: process.env.FEISHU_BASE_URL?.trim() || "https://open.feishu.cn"
  };
}

export type IpGuardConfig = {
  url: string;
  name: string;
  password: string;
};

export function getIpGuardConfig(): IpGuardConfig {
  return {
    url: process.env.IPGUARD_URL ?? "http://192.168.10.30:8095",
    name: process.env.IPGUARD_NAME ?? "ipguard-dify",
    password: process.env.IPGUARD_PASSWORD ?? "IPGUARD#dify202509",
  };
}

export type AiCodingApiConfig = {
  baseUrl: string;
  userId: string;
  demandListPath: string;
  demandBindPath: string;
  roundRecordPath: string;
  roundRevertPath: string;
};

function requireEnv(name: string, fallback?: string): string {
  const value = process.env[name]?.trim() || fallback;
  if (!value) {
    throw new Error(`${name} is required`);
  }

  return value;
}

export function getAiCodingApiConfig(): AiCodingApiConfig {
  const baseUrl = requireEnv("AI_CODING_STATS_API_BASE_URL", "http://127.0.0.1:8080").replace(/\/+$/, "");

  return {
    baseUrl,
    userId: requireEnv("AI_CODING_USER_ID"),
    demandListPath: process.env.AI_CODING_DEMAND_LIST_PATH?.trim() || "/ai-coding/mcp/demand/list",
    demandBindPath: process.env.AI_CODING_DEMAND_BIND_PATH?.trim() || "/ai-coding/mcp/demand/bind",
    roundRecordPath: process.env.AI_CODING_ROUND_RECORD_PATH?.trim() || "/ai-coding/mcp/round/record",
    roundRevertPath: process.env.AI_CODING_ROUND_REVERT_PATH?.trim() || "/ai-coding/mcp/round/revert"
  };
}

import { appendFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type EventLogPayload = Record<string, unknown>;

const moduleDir = dirname(fileURLToPath(import.meta.url));
const logDir = resolve(moduleDir, "../logs");
const logFilePath = resolve(logDir, "ai-coding-events.jsonl");

export async function logEvent(type: string, payload: EventLogPayload): Promise<void> {
  const record = {
    timestamp: new Date().toISOString(),
    type,
    ...payload
  };

  try {
    await mkdir(logDir, { recursive: true });
    await appendFile(logFilePath, `${JSON.stringify(record)}\n`, "utf8");
  } catch (error) {
    console.error(
      `[event-log] failed to append ${type}:`,
      error instanceof Error ? error.message : String(error)
    );
  }
}

export function getEventLogFilePath(): string {
  return logFilePath;
}

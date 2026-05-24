import "dotenv/config";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";

const env: Record<string, string> = {};
for (const [key, value] of Object.entries(process.env)) {
  if (value !== undefined) {
    env[key] = value;
  }
}

const transport = new StdioClientTransport({
  command: "node",
  args: ["dist/index.js"],
  cwd: process.cwd(),
  env,
  stderr: "pipe"
});

const client = new Client({
  name: "mcp-toolbox-round-record-verify",
  version: "0.1.0"
});

const now = Date.now();
const conversationId = `verify-round-${now}`;

try {
  await client.connect(transport);

  const result = await client.callTool({
    name: "record_ai_coding_round",
    arguments: {
      conversationId,
      startedAt: new Date(now - 15_000).toISOString(),
      endedAt: new Date(now).toISOString(),
      modelName: "gpt-5-codex",
      promptText: process.argv[2] ?? "验证新会话 MCP round record 链路",
      filesChanged: 0,
      linesAdded: 0,
      linesDeleted: 0,
      codeLinesChanged: 0,
      inputTokens: 0,
      outputTokens: 0,
      totalTokens: 0,
      metadata: {
        client: "local-verify",
        codeStatsSource: "manual",
        tokenStatsUnavailable: true
      }
    }
  });

  console.log(JSON.stringify(result.structuredContent, null, 2));
} finally {
  await client.close();
}

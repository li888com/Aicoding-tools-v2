import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { bindDemand, listDemands } from "../database.js";

const stringField = z.string().min(1);

export function registerDemandBindingTools(server: McpServer): void {
  server.tool(
    "ai_coding_list_demands",
    "List current user's demands from the configured AI Coding backend API.",
    {
      userId: z.string().optional().describe("Optional override for the configured user id.")
    },
    async (input) => {
      const result = await listDemands(input.userId);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2)
          }
        ],
        structuredContent: result
      };
    }
  );

  server.tool(
    "ai_coding_bind_demand",
    "Bind a conversation to a concrete demand for the configured user.",
    {
      conversationId: stringField.describe("Stable id for the AI Coding conversation/thread."),
      demandId: stringField.describe("Demand id returned by the backend demand list API."),
      demandCode: z.string().optional().describe("Optional demand code for display and fallback matching."),
      demandName: z.string().optional().describe("Optional demand name for display."),
      userId: z.string().optional().describe("Optional override for the configured user id."),
      client: z.string().optional().describe("Client name such as codex or claude-code.")
    },
    async (input) => {
      const result = await bindDemand(input);

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(result, null, 2)
          }
        ],
        structuredContent: result
      };
    }
  );
}

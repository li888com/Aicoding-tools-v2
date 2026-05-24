import { closePool, getPool, recordRound, recordRoundRevert } from "../src/database.js";

const conversationId = `verify-revert-${Date.now()}`;
const now = Date.now();

try {
  const round = await recordRound({
    conversationId,
    startedAt: new Date(now - 120_000).toISOString(),
    endedAt: new Date(now - 90_000).toISOString(),
    modelName: "verify-model",
    promptText: "实现 #56 的待撤销改动",
    filesChanged: 2,
    linesAdded: 20,
    linesDeleted: 4,
    inputTokens: 100,
    outputTokens: 50
  });

  const revert = await recordRoundRevert({
    conversationId,
    targetRoundId: round.id,
    revertedAt: new Date(now - 30_000).toISOString(),
    modelName: "verify-model",
    promptText: "撤销上一轮代码改动",
    reason: "verification",
    filesChanged: 2,
    linesAdded: 4,
    linesDeleted: 20,
    inputTokens: 80,
    outputTokens: 30,
    metadata: {
      client: "verify-script"
    }
  });

  const [effectiveRows] = await getPool().execute(
    `SELECT id
     FROM ai_coding_effective_rounds
     WHERE id = :id`,
    { id: round.id }
  );

  if (Array.isArray(effectiveRows) && effectiveRows.length !== 0) {
    throw new Error(`Round ${round.id} should be excluded from ai_coding_effective_rounds`);
  }

  console.log(
    JSON.stringify(
      {
        ok: true,
        round,
        revert,
        effectiveRoundExcluded: true
      },
      null,
      2
    )
  );
} finally {
  await closePool();
}

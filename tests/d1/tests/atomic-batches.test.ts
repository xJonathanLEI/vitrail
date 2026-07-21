import assert from "node:assert/strict";

import { fetchJson, resetDatabase, test } from "./fixture";

interface SuccessfulBatchResponse {
	ok: boolean;
	emptyBatch: boolean;
	insertedId: string;
	manyCount: number;
	optionalId: string;
	firstId: string;
	updatedCount: number;
	deletedCount: number;
	sessionInsertedId: string;
	sessionBookmark: string;
	acceptedBindingCount: number;
	acceptedBindingRows: number;
	acceptedBindingIdSum: string;
	rejectedBindingError: string;
}

interface HighLevelBatchResponse {
	ok: boolean;
	directInsertedId: string;
	directInsertedName: string;
	directBeforeId: string;
	directBeforeName: string;
	directUpdatedCount: number;
	directAfterId: string;
	directAfterName: string;
	directDeletedCount: number;
	directRemainingRows: number;
	sessionInsertedId: string;
	sessionInsertedName: string;
	sessionQueriedId: string;
	sessionQueriedName: string;
	sessionDeletedCount: number;
	sessionBookmark: string;
	sessionRemainingRows: number;
	compileError: string;
	queueFailureRemainingRows: number;
}

interface RollbackResponse {
	ok: boolean;
	bindingError: string;
	remainingRows: number;
	error: string;
}

interface DecodeFailureResponse {
	ok: boolean;
	error: string;
}

test("high-level D1 batches infer anonymous tuple outputs and preserve ordered execution", async ({
	miniflare,
}) => {
	const batch = await fetchJson<HighLevelBatchResponse>(
		miniflare,
		"/__test/high-level-batches",
		{
			method: "POST",
		},
	);

	assert.equal(batch.ok, true);

	assert.match(batch.directInsertedId, /^[1-9][0-9]*$/);
	assert.equal(batch.directInsertedName, "high-level-direct-before");
	assert.equal(batch.directBeforeId, batch.directInsertedId);
	assert.equal(batch.directBeforeName, "high-level-direct-before");
	assert.equal(batch.directUpdatedCount, 1);
	assert.equal(batch.directAfterId, batch.directInsertedId);
	assert.equal(batch.directAfterName, "high-level-direct-after");
	assert.equal(batch.directDeletedCount, 1);
	assert.equal(batch.directRemainingRows, 0);

	assert.match(batch.sessionInsertedId, /^[1-9][0-9]*$/);
	assert.equal(batch.sessionInsertedName, "high-level-session-author");
	assert.equal(batch.sessionQueriedId, batch.sessionInsertedId);
	assert.equal(batch.sessionQueriedName, "high-level-session-author");
	assert.equal(batch.sessionDeletedCount, 1);
	assert.equal(typeof batch.sessionBookmark, "string");
	assert.notEqual(batch.sessionBookmark.length, 0);
	assert.equal(batch.sessionRemainingRows, 0);

	assert.match(batch.compileError, /101 bound parameters/);
	assert.match(batch.compileError, /allowed limit of 100/);
	assert.equal(batch.queueFailureRemainingRows, 0);
});

test("typed D1 atomic batches preserve outputs, roll back, enforce limits, and decode safely", async ({
	miniflare,
}) => {
	const successfulBatch = await fetchJson<SuccessfulBatchResponse>(
		miniflare,
		"/__test/atomic-batches",
		{
			method: "POST",
		},
	);

	assert.equal(successfulBatch.ok, true);
	assert.equal(successfulBatch.emptyBatch, true);
	assert.match(successfulBatch.insertedId, /^[1-9][0-9]*$/);
	assert.equal(successfulBatch.manyCount, 1);
	assert.equal(successfulBatch.optionalId, successfulBatch.insertedId);
	assert.equal(successfulBatch.firstId, successfulBatch.insertedId);
	assert.equal(successfulBatch.updatedCount, 1);
	assert.equal(successfulBatch.deletedCount, 1);
	assert.match(successfulBatch.sessionInsertedId, /^[1-9][0-9]*$/);
	assert.equal(typeof successfulBatch.sessionBookmark, "string");
	assert.notEqual(successfulBatch.sessionBookmark.length, 0);
	assert.equal(successfulBatch.acceptedBindingCount, 100);
	assert.equal(successfulBatch.acceptedBindingRows, 1);
	assert.equal(
		successfulBatch.acceptedBindingIdSum,
		successfulBatch.sessionInsertedId,
	);
	assert.match(successfulBatch.rejectedBindingError, /101 bound parameters/);
	assert.match(successfulBatch.rejectedBindingError, /allowed limit of 100/);

	await resetDatabase(miniflare);

	const rollback = await fetchJson<RollbackResponse>(
		miniflare,
		"/__test/atomic-batch-rollback",
		{
			method: "POST",
		},
	);

	assert.equal(rollback.ok, true);
	assert.match(rollback.bindingError, /floating-point bindings must be finite/);
	assert.equal(rollback.remainingRows, 0);
	assert.equal(typeof rollback.error, "string");
	assert.notEqual(rollback.error.length, 0);

	await resetDatabase(miniflare);

	const decodeFailure = await fetchJson<DecodeFailureResponse>(
		miniflare,
		"/__test/atomic-batch-decode-error",
		{
			method: "POST",
		},
	);

	assert.equal(decodeFailure.ok, true);
	assert.match(decodeFailure.error, /failed to decode D1 result/);
	assert.match(decodeFailure.error, /not-an-integer/);
});

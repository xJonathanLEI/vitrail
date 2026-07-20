import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Miniflare } from "miniflare";

const fixtureDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workerScript = join(fixtureDirectory, "build", "worker", "shim.mjs");
const expectedWorkerBuildVersion = "0.8.5";

async function fetchJson(miniflare, path, init) {
	const response = await miniflare.dispatchFetch(
		`http://vitrail.test${path}`,
		init,
	);
	const body = await response.text();

	assert.equal(
		response.status,
		200,
		`request to ${path} failed with ${response.status}: ${body}`,
	);

	return JSON.parse(body);
}

async function resetDatabase(miniflare) {
	assert.deepEqual(
		await fetchJson(miniflare, "/__test/setup", {
			method: "POST",
		}),
		{ ok: true },
	);
}

test("typed D1 atomic batches preserve outputs, roll back, enforce limits, and decode safely", {
	timeout: 600_000,
}, async () => {
	const workerBuildVersion = execFileSync("worker-build", ["--version"], {
		encoding: "utf8",
	}).trim();

	assert.equal(
		workerBuildVersion,
		expectedWorkerBuildVersion,
		`expected worker-build ${expectedWorkerBuildVersion}, got ${workerBuildVersion}`,
	);

	execFileSync(
		"worker-build",
		["--release", "--no-panic-recovery", "--features", "integration-test"],
		{
			cwd: fixtureDirectory,
			stdio: "inherit",
		},
	);

	const persistenceDirectory = await mkdtemp(
		join(tmpdir(), "vitrail-d1-atomic-batches-"),
	);
	let miniflare;

	try {
		miniflare = new Miniflare({
			compatibilityDate: "2026-07-14",
			d1Databases: {
				DB: `vitrail-atomic-batch-test-${randomUUID()}`,
			},
			d1Persist: persistenceDirectory,
			modules: true,
			modulesRules: [
				{
					include: ["**/*.wasm"],
					type: "CompiledWasm",
				},
			],
			scriptPath: workerScript,
		});

		await miniflare.ready;

		await resetDatabase(miniflare);

		const successfulBatch = await fetchJson(
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

		const rollback = await fetchJson(
			miniflare,
			"/__test/atomic-batch-rollback",
			{
				method: "POST",
			},
		);

		assert.equal(rollback.ok, true);
		assert.match(
			rollback.bindingError,
			/floating-point bindings must be finite/,
		);
		assert.equal(rollback.remainingRows, 0);
		assert.equal(typeof rollback.error, "string");
		assert.notEqual(rollback.error.length, 0);

		await resetDatabase(miniflare);

		const decodeFailure = await fetchJson(
			miniflare,
			"/__test/atomic-batch-decode-error",
			{
				method: "POST",
			},
		);

		assert.equal(decodeFailure.ok, true);
		assert.match(decodeFailure.error, /failed to decode D1 result/);
		assert.match(decodeFailure.error, /not-an-integer/);
	} finally {
		try {
			await miniflare?.dispose();
		} finally {
			await rm(persistenceDirectory, {
				force: true,
				recursive: true,
			});
		}
	}
});

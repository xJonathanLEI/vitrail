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

test("explicit D1 sessions preserve sequential consistency and bookmarks", {
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
		join(tmpdir(), "vitrail-d1-sessions-"),
	);
	let miniflare;

	try {
		miniflare = new Miniflare({
			compatibilityDate: "2026-07-14",
			d1Databases: {
				DB: `vitrail-session-test-${randomUUID()}`,
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

		assert.deepEqual(
			await fetchJson(miniflare, "/__test/setup", {
				method: "POST",
			}),
			{ ok: true },
		);

		const result = await fetchJson(miniflare, "/__test/sessions", {
			method: "POST",
		});

		assert.equal(result.ok, true);
		assert.match(result.insertedId, /^[1-9][0-9]*$/);
		assert.equal(typeof result.initialBookmark, "string");
		assert.notEqual(result.initialBookmark.length, 0);
		assert.equal(typeof result.advancedBookmark, "string");
		assert.notEqual(result.advancedBookmark.length, 0);
		assert.notEqual(result.advancedBookmark, result.initialBookmark);
		assert.equal(result.bookmarkReadNote, "session-updated");
		assert.equal(result.updatedCount, 1);
		assert.equal(typeof result.unconstrainedBookmark, "string");
		assert.notEqual(result.unconstrainedBookmark.length, 0);
		assert.equal(result.sequentialReadCount, 2);
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

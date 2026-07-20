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

test("direct D1 CRUD preserves every supported scalar transport", {
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
		join(tmpdir(), "vitrail-d1-miniflare-"),
	);
	let miniflare;

	try {
		miniflare = new Miniflare({
			compatibilityDate: "2026-07-14",
			d1Databases: {
				DB: `vitrail-test-${randomUUID()}`,
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

		const result = await fetchJson(miniflare, "/__test/crud", {
			method: "POST",
		});

		assert.equal(result.ok, true);
		assert.equal(result.inserted.minValue, "-9223372036854775808");
		assert.equal(result.inserted.maxValue, "9223372036854775807");
		assert.equal(result.inserted.active, true);
		assert.equal(result.inserted.score, 1234.5);
		assert.equal(result.inserted.label, "edge-values");
		assert.deepEqual(result.inserted.payload, [0, 1, 2, 127, 128, 254, 255]);
		assert.equal(result.inserted.createdAt, "2026-07-14T12:34:56.123456+00:00");
		assert.deepEqual(result.inserted.metadata, {
			kind: "d1-probe",
			nested: {
				enabled: true,
				count: 7,
			},
		});
		assert.equal(result.inserted.note, null);
		assert.equal(result.queried.id, result.inserted.id);
		assert.equal(result.queried.minValue, "-9223372036854775808");
		assert.equal(result.queried.maxValue, "9223372036854775807");
		assert.equal(result.updatedCount, 1);
		assert.equal(result.deletedCount, 1);
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

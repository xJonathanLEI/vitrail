import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { Miniflare } from "miniflare";
import { test as base } from "vitest";

const fixtureDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const workerScript = join(fixtureDirectory, "build", "worker", "shim.mjs");

type DispatchFetchInit = Parameters<Miniflare["dispatchFetch"]>[1];

interface MiniflareFixtures {
	miniflare: Miniflare;
}

interface SetupResponse {
	ok: boolean;
}

export async function fetchJson<ResponseBody>(
	miniflare: Miniflare,
	path: string,
	init: DispatchFetchInit,
): Promise<ResponseBody> {
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

	return JSON.parse(body) as ResponseBody;
}

export async function resetDatabase(miniflare: Miniflare): Promise<void> {
	assert.deepEqual(
		await fetchJson<SetupResponse>(miniflare, "/__test/setup", {
			method: "POST",
		}),
		{ ok: true },
	);
}

export const test = base.extend<MiniflareFixtures>({
	miniflare: async ({ task }, use): Promise<void> => {
		const testName = task.name
			.toLowerCase()
			.replaceAll(/[^a-z0-9]+/g, "-")
			.replaceAll(/(^-|-$)/g, "")
			.slice(0, 48);
		const persistenceDirectory = await mkdtemp(
			join(tmpdir(), `vitrail-d1-${testName}-`),
		);
		let miniflare: Miniflare | undefined;

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
			await resetDatabase(miniflare);
			await use(miniflare);
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
	},
});

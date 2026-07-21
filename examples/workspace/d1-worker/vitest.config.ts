import { fileURLToPath } from "node:url";

import { defineConfig } from "vitest/config";

const cacheDir = fileURLToPath(
	new URL("../../../node_modules/.vite/d1-worker", import.meta.url),
);
const root = fileURLToPath(new URL(".", import.meta.url));
const globalSetup = fileURLToPath(
	new URL("./tests/global-setup.ts", import.meta.url),
);

export default defineConfig({
	cacheDir,
	root,
	test: {
		fileParallelism: false,
		hookTimeout: 600_000,
		testTimeout: 600_000,
		globalSetup,
		include: [
			"tests/atomic-batches.test.ts",
			"tests/direct-crud.test.ts",
			"tests/sessions.test.ts",
		],
	},
});

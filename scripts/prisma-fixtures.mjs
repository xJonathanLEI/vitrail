#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_POSTGRES_URL =
	"postgres://postgres:postgres@127.0.0.1:5432/vitrail";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");
const fixturesDir = resolve(
	repoRoot,
	"vitrail-pg/tests/fixtures/pg_migrations",
);

const paths = {
	prismaConfig: resolve(fixturesDir, "prisma.config.ts"),
	baseSchema: resolve(fixturesDir, "base.prisma"),
	expandedSchema: resolve(fixturesDir, "expanded.prisma"),
	emptyToBaseSql: resolve(fixturesDir, "empty_to_base.sql"),
	baseToExpandedSql: resolve(fixturesDir, "base_to_expanded.sql"),
	emptyToExpandedSql: resolve(fixturesDir, "empty_to_expanded.sql"),
};

const baseDatabaseUrl =
	process.env.VITRAIL_POSTGRES_URL ??
	process.env.DATABASE_URL ??
	DEFAULT_POSTGRES_URL;

main().catch((error) => {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
});

async function main() {
	const emptyToBaseDatabase = createTemporaryDatabase(baseDatabaseUrl);
	const baseToExpandedDatabase = createTemporaryDatabase(baseDatabaseUrl);
	const emptyToExpandedDatabase = createTemporaryDatabase(baseDatabaseUrl);

	try {
		const generated = {
			emptyToBaseSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						paths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						paths.baseSchema,
						"--script",
					],
					emptyToBaseDatabase.databaseUrl,
				),
			),
			baseToExpandedSql: "",
			emptyToExpandedSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						paths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						paths.expandedSchema,
						"--script",
					],
					emptyToExpandedDatabase.databaseUrl,
				),
			),
		};

		runPrisma(
			[
				"db",
				"push",
				"--config",
				paths.prismaConfig,
				"--schema",
				paths.baseSchema,
			],
			baseToExpandedDatabase.databaseUrl,
		);

		generated.baseToExpandedSql = normalizeSql(
			runPrisma(
				[
					"migrate",
					"diff",
					"--config",
					paths.prismaConfig,
					"--from-config-datasource",
					"--to-schema",
					paths.expandedSchema,
					"--script",
				],
				baseToExpandedDatabase.databaseUrl,
			),
		);

		writeFileSync(paths.emptyToBaseSql, generated.emptyToBaseSql);
		writeFileSync(paths.baseToExpandedSql, generated.baseToExpandedSql);
		writeFileSync(paths.emptyToExpandedSql, generated.emptyToExpandedSql);

		console.log("Prisma fixtures regenerated.");
	} finally {
		await Promise.allSettled([
			emptyToBaseDatabase.cleanup(),
			baseToExpandedDatabase.cleanup(),
			emptyToExpandedDatabase.cleanup(),
		]);
	}
}

function runPrisma(args, databaseUrl) {
	const result = spawnSync("npx", ["--yes", "prisma", ...args], {
		cwd: repoRoot,
		env: {
			...process.env,
			DATABASE_URL: databaseUrl,
		},
		encoding: "utf8",
	});

	if (result.error) {
		throw new Error(
			[
				`Failed to invoke Prisma CLI: ${result.error.message}`,
				postgresSetupHelp(baseDatabaseUrl),
			].join("\n\n"),
		);
	}

	if (result.status !== 0) {
		throw new Error(
			[
				`Prisma CLI failed with exit code ${result.status ?? "unknown"}.`,
				result.stdout ? `stdout:\n${result.stdout}` : null,
				result.stderr ? `stderr:\n${result.stderr}` : null,
				postgresSetupHelp(baseDatabaseUrl),
			]
				.filter(Boolean)
				.join("\n\n"),
		);
	}

	return result.stdout;
}

function runPrismaDbExecute(databaseUrl, sql, helpDatabaseUrl) {
	const result = spawnSync(
		"npx",
		[
			"--yes",
			"prisma",
			"db",
			"execute",
			"--config",
			paths.prismaConfig,
			"--stdin",
		],
		{
			cwd: repoRoot,
			env: {
				...process.env,
				DATABASE_URL: databaseUrl,
			},
			input: sql,
			encoding: "utf8",
		},
	);

	if (result.error) {
		throw new Error(
			[
				`Failed to invoke Prisma db execute: ${result.error.message}`,
				postgresSetupHelp(helpDatabaseUrl),
			].join("\n\n"),
		);
	}

	if (result.status !== 0) {
		throw new Error(
			[
				`Prisma db execute failed with exit code ${result.status ?? "unknown"}.`,
				result.stdout ? `stdout:\n${result.stdout}` : null,
				result.stderr ? `stderr:\n${result.stderr}` : null,
				postgresSetupHelp(helpDatabaseUrl),
			]
				.filter(Boolean)
				.join("\n\n"),
		);
	}
}

function normalizeSql(sql) {
	const normalized = sql.replace(/\r\n/g, "\n");
	return normalized.endsWith("\n") ? normalized : `${normalized}\n`;
}

function createTemporaryDatabase(baseUrl) {
	const databaseName = `vitrail_${process.pid}_${Date.now()}_${Math.random()
		.toString(36)
		.slice(2, 10)}`;
	const adminDatabaseUrl = replaceDatabaseName(baseUrl, "postgres");
	const databaseUrl = replaceDatabaseName(baseUrl, databaseName);

	runPrismaDbExecute(
		adminDatabaseUrl,
		`CREATE DATABASE "${databaseName}";`,
		baseUrl,
	);

	return {
		databaseUrl,
		async cleanup() {
			try {
				runPrismaDbExecute(
					adminDatabaseUrl,
					`
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${databaseName}'
  AND pid <> pg_backend_pid();
`,
					baseUrl,
				);
				runPrismaDbExecute(
					adminDatabaseUrl,
					`DROP DATABASE "${databaseName}";`,
					baseUrl,
				);
			} catch (error) {
				throw new Error(
					`Failed to clean up temporary database "${databaseName}": ${formatError(error)}`,
				);
			}
		},
	};
}

function replaceDatabaseName(databaseUrl, databaseName) {
	const url = new URL(databaseUrl);
	url.pathname = `/${databaseName}`;
	return url.toString();
}

function postgresSetupHelp(databaseUrl) {
	return [
		"Prisma fixture generation requires an existing Postgres server.",
		`Expected base URL: ${DEFAULT_POSTGRES_URL}`,
		`Resolved base URL: ${databaseUrl}`,
		"You can override it with VITRAIL_POSTGRES_URL or DATABASE_URL.",
		"Example:",
		"  docker run --rm -e POSTGRES_USER=postgres \\",
		"    -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=vitrail \\",
		"    -p 127.0.0.1:5432:5432 postgres:16-alpine",
	].join("\n");
}

function formatError(error) {
	if (error instanceof Error) {
		return error.message;
	}

	return String(error);
}

#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DEFAULT_POSTGRES_URL =
	"postgres://postgres:postgres@127.0.0.1:5432/vitrail";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const repoRoot = resolve(__dirname, "..");

const postgresFixturesDir = resolve(
	repoRoot,
	"vitrail-pg/tests/fixtures/pg_migrations",
);
const sqliteFixturesDir = resolve(
	repoRoot,
	"vitrail-sqlite/tests/fixtures/sqlite_migrations",
);

const postgresPaths = fixturePaths(postgresFixturesDir);
const sqlitePaths = fixturePaths(sqliteFixturesDir);

const basePostgresUrl =
	process.env.VITRAIL_POSTGRES_URL ??
	process.env.DATABASE_URL ??
	DEFAULT_POSTGRES_URL;

const command = process.argv[2] ?? "generate";

main(command).catch((error) => {
	console.error(error instanceof Error ? error.message : String(error));
	process.exit(1);
});

async function main(requestedCommand) {
	switch (requestedCommand) {
		case "generate":
		case "all":
			await generatePostgresFixtures();
			await generateSqliteFixtures();
			console.log("Prisma fixtures regenerated.");
			break;
		case "postgres":
			await generatePostgresFixtures();
			console.log("PostgreSQL Prisma fixtures regenerated.");
			break;
		case "sqlite":
			await generateSqliteFixtures();
			console.log("SQLite Prisma fixtures regenerated.");
			break;
		default:
			throw new Error(
				`Unknown fixture generation command \`${requestedCommand}\`. Expected \`generate\`, \`all\`, \`postgres\`, or \`sqlite\`.`,
			);
	}
}

async function generatePostgresFixtures() {
	const emptyToBaseDatabase = createTemporaryPostgresDatabase(basePostgresUrl);
	const baseToExpandedDatabase =
		createTemporaryPostgresDatabase(basePostgresUrl);
	const emptyToExpandedDatabase =
		createTemporaryPostgresDatabase(basePostgresUrl);
	const externalOnlyToBaseDatabase =
		createTemporaryPostgresDatabase(basePostgresUrl);
	const emptyToBigintDatabase =
		createTemporaryPostgresDatabase(basePostgresUrl);
	const emptyToOptionalOneToOneDatabase =
		createTemporaryPostgresDatabase(basePostgresUrl);

	try {
		const generated = {
			emptyToBaseSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						postgresPaths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						postgresPaths.baseSchema,
						"--script",
					],
					emptyToBaseDatabase.databaseUrl,
					postgresSetupHelp(basePostgresUrl),
				),
			),
			baseToExpandedSql: "",
			emptyToExpandedSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						postgresPaths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						postgresPaths.expandedSchema,
						"--script",
					],
					emptyToExpandedDatabase.databaseUrl,
					postgresSetupHelp(basePostgresUrl),
				),
			),
			externalOnlyToBaseSql: "",
			emptyToBigintSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						postgresPaths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						postgresPaths.bigintSchema,
						"--script",
					],
					emptyToBigintDatabase.databaseUrl,
					postgresSetupHelp(basePostgresUrl),
				),
			),
			emptyToOptionalOneToOneSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						postgresPaths.prismaConfig,
						"--from-config-datasource",
						"--to-schema",
						postgresPaths.optionalOneToOneSchema,
						"--script",
					],
					emptyToOptionalOneToOneDatabase.databaseUrl,
					postgresSetupHelp(basePostgresUrl),
				),
			),
		};

		runPrisma(
			[
				"db",
				"push",
				"--config",
				postgresPaths.prismaConfig,
				"--schema",
				postgresPaths.baseSchema,
			],
			baseToExpandedDatabase.databaseUrl,
			postgresSetupHelp(basePostgresUrl),
		);

		runPrismaDbExecute({
			configPath: postgresPaths.prismaConfig,
			databaseUrl: externalOnlyToBaseDatabase.databaseUrl,
			sql: `
CREATE TABLE public.external_audit_log (
  id SERIAL PRIMARY KEY,
  payload TEXT NOT NULL
);
`,
			help: postgresSetupHelp(basePostgresUrl),
		});

		generated.externalOnlyToBaseSql = normalizeSql(
			runPrisma(
				[
					"migrate",
					"diff",
					"--config",
					postgresPaths.externalPrismaConfig,
					"--from-config-datasource",
					"--to-schema",
					postgresPaths.baseSchema,
					"--script",
				],
				externalOnlyToBaseDatabase.databaseUrl,
				postgresSetupHelp(basePostgresUrl),
			),
		);

		generated.baseToExpandedSql = normalizeSql(
			runPrisma(
				[
					"migrate",
					"diff",
					"--config",
					postgresPaths.prismaConfig,
					"--from-config-datasource",
					"--to-schema",
					postgresPaths.expandedSchema,
					"--script",
				],
				baseToExpandedDatabase.databaseUrl,
				postgresSetupHelp(basePostgresUrl),
			),
		);

		writeGeneratedFixtures(postgresPaths, generated);
	} finally {
		await Promise.allSettled([
			emptyToBaseDatabase.cleanup(),
			baseToExpandedDatabase.cleanup(),
			emptyToExpandedDatabase.cleanup(),
			externalOnlyToBaseDatabase.cleanup(),
			emptyToBigintDatabase.cleanup(),
			emptyToOptionalOneToOneDatabase.cleanup(),
		]);
	}
}

async function generateSqliteFixtures() {
	const emptyToBaseDatabase = createTemporarySqliteDatabase("empty_to_base");
	const baseToExpandedDatabase =
		createTemporarySqliteDatabase("base_to_expanded");
	const emptyToExpandedDatabase =
		createTemporarySqliteDatabase("empty_to_expanded");
	const externalOnlyToBaseDatabase = createTemporarySqliteDatabase(
		"external_only_to_base",
	);
	const emptyToBigintDatabase =
		createTemporarySqliteDatabase("empty_to_bigint");
	const emptyToOptionalOneToOneDatabase = createTemporarySqliteDatabase(
		"empty_to_optional_one_to_one",
	);

	try {
		const generated = {
			emptyToBaseSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						sqlitePaths.prismaConfig,
						"--from-empty",
						"--to-schema",
						sqlitePaths.baseSchema,
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
						sqlitePaths.prismaConfig,
						"--from-empty",
						"--to-schema",
						sqlitePaths.expandedSchema,
						"--script",
					],
					emptyToExpandedDatabase.databaseUrl,
				),
			),
			externalOnlyToBaseSql: "",
			emptyToBigintSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						sqlitePaths.prismaConfig,
						"--from-empty",
						"--to-schema",
						sqlitePaths.bigintSchema,
						"--script",
					],
					emptyToBigintDatabase.databaseUrl,
				),
			),
			emptyToOptionalOneToOneSql: normalizeSql(
				runPrisma(
					[
						"migrate",
						"diff",
						"--config",
						sqlitePaths.prismaConfig,
						"--from-empty",
						"--to-schema",
						sqlitePaths.optionalOneToOneSchema,
						"--script",
					],
					emptyToOptionalOneToOneDatabase.databaseUrl,
				),
			),
		};

		runPrisma(
			[
				"db",
				"push",
				"--config",
				sqlitePaths.prismaConfig,
				"--schema",
				sqlitePaths.baseSchema,
			],
			baseToExpandedDatabase.databaseUrl,
		);

		runPrismaDbExecute({
			configPath: sqlitePaths.prismaConfig,
			databaseUrl: externalOnlyToBaseDatabase.databaseUrl,
			sql: `
CREATE TABLE "external_audit_log" (
  "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  "payload" TEXT NOT NULL
);
`,
		});

		generated.externalOnlyToBaseSql = normalizeSql(
			runPrisma(
				[
					"migrate",
					"diff",
					"--config",
					sqlitePaths.externalPrismaConfig,
					"--from-config-datasource",
					"--to-schema",
					sqlitePaths.baseSchema,
					"--script",
				],
				externalOnlyToBaseDatabase.databaseUrl,
			),
		);

		generated.baseToExpandedSql = normalizeSql(
			runPrisma(
				[
					"migrate",
					"diff",
					"--config",
					sqlitePaths.prismaConfig,
					"--from-config-datasource",
					"--to-schema",
					sqlitePaths.expandedSchema,
					"--script",
				],
				baseToExpandedDatabase.databaseUrl,
			),
		);

		writeGeneratedFixtures(sqlitePaths, generated);
	} finally {
		await Promise.allSettled([
			emptyToBaseDatabase.cleanup(),
			baseToExpandedDatabase.cleanup(),
			emptyToExpandedDatabase.cleanup(),
			externalOnlyToBaseDatabase.cleanup(),
			emptyToBigintDatabase.cleanup(),
			emptyToOptionalOneToOneDatabase.cleanup(),
		]);
	}
}

function fixturePaths(fixturesDirectory) {
	return {
		prismaConfig: resolve(fixturesDirectory, "prisma.config.ts"),
		externalPrismaConfig: resolve(
			fixturesDirectory,
			"prisma.external.config.ts",
		),
		baseSchema: resolve(fixturesDirectory, "base.prisma"),
		expandedSchema: resolve(fixturesDirectory, "expanded.prisma"),
		bigintSchema: resolve(fixturesDirectory, "bigint.prisma"),
		optionalOneToOneSchema: resolve(
			fixturesDirectory,
			"optional_one_to_one.prisma",
		),
		emptyToBaseSql: resolve(fixturesDirectory, "empty_to_base.sql"),
		baseToExpandedSql: resolve(fixturesDirectory, "base_to_expanded.sql"),
		emptyToExpandedSql: resolve(fixturesDirectory, "empty_to_expanded.sql"),
		externalOnlyToBaseSql: resolve(
			fixturesDirectory,
			"external_only_to_base.sql",
		),
		emptyToBigintSql: resolve(fixturesDirectory, "empty_to_bigint.sql"),
		emptyToOptionalOneToOneSql: resolve(
			fixturesDirectory,
			"empty_to_optional_one_to_one.sql",
		),
	};
}

function writeGeneratedFixtures(paths, generated) {
	writeFileSync(paths.emptyToBaseSql, generated.emptyToBaseSql);
	writeFileSync(paths.baseToExpandedSql, generated.baseToExpandedSql);
	writeFileSync(paths.emptyToExpandedSql, generated.emptyToExpandedSql);
	writeFileSync(paths.externalOnlyToBaseSql, generated.externalOnlyToBaseSql);
	writeFileSync(paths.emptyToBigintSql, generated.emptyToBigintSql);
	writeFileSync(
		paths.emptyToOptionalOneToOneSql,
		generated.emptyToOptionalOneToOneSql,
	);
}

function runPrisma(args, databaseUrl, help) {
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
			formatCommandError(
				`Failed to invoke Prisma CLI: ${result.error.message}`,
				result,
				help,
			),
		);
	}

	if (result.status !== 0) {
		throw new Error(
			formatCommandError(
				`Prisma CLI failed with exit code ${result.status ?? "unknown"}.`,
				result,
				help,
			),
		);
	}

	return result.stdout;
}

function runPrismaDbExecute({ configPath, databaseUrl, sql, help }) {
	const result = spawnSync(
		"npx",
		["--yes", "prisma", "db", "execute", "--config", configPath, "--stdin"],
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
			formatCommandError(
				`Failed to invoke Prisma db execute: ${result.error.message}`,
				result,
				help,
			),
		);
	}

	if (result.status !== 0) {
		throw new Error(
			formatCommandError(
				`Prisma db execute failed with exit code ${result.status ?? "unknown"}.`,
				result,
				help,
			),
		);
	}
}

function formatCommandError(message, result, help) {
	return [
		message,
		result.stdout ? `stdout:\n${result.stdout}` : null,
		result.stderr ? `stderr:\n${result.stderr}` : null,
		help,
	]
		.filter(Boolean)
		.join("\n\n");
}

function normalizeSql(sql) {
	const normalized = sql.replace(/\r\n/g, "\n");
	return normalized.endsWith("\n") ? normalized : `${normalized}\n`;
}

function createTemporaryPostgresDatabase(baseUrl) {
	const databaseName = `vitrail_${process.pid}_${Date.now()}_${Math.random()
		.toString(36)
		.slice(2, 10)}`;
	const adminDatabaseUrl = replaceDatabaseName(baseUrl, "postgres");
	const databaseUrl = replaceDatabaseName(baseUrl, databaseName);

	runPrismaDbExecute({
		configPath: postgresPaths.prismaConfig,
		databaseUrl: adminDatabaseUrl,
		sql: `CREATE DATABASE "${databaseName}";`,
		help: postgresSetupHelp(baseUrl),
	});

	return {
		databaseUrl,
		async cleanup() {
			try {
				runPrismaDbExecute({
					configPath: postgresPaths.prismaConfig,
					databaseUrl: adminDatabaseUrl,
					sql: `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${databaseName}'
  AND pid <> pg_backend_pid();
`,
					help: postgresSetupHelp(baseUrl),
				});
				runPrismaDbExecute({
					configPath: postgresPaths.prismaConfig,
					databaseUrl: adminDatabaseUrl,
					sql: `DROP DATABASE "${databaseName}";`,
					help: postgresSetupHelp(baseUrl),
				});
			} catch (error) {
				throw new Error(
					`Failed to clean up temporary database "${databaseName}": ${formatError(error)}`,
				);
			}
		},
	};
}

function createTemporarySqliteDatabase(label) {
	const directory = mkdtempSync(
		resolve(tmpdir(), `vitrail_sqlite_${label}_${process.pid}_`),
	);
	const databasePath = resolve(directory, "database.db");

	return {
		databaseUrl: `file:${databasePath}`,
		async cleanup() {
			try {
				rmSync(directory, { force: true, recursive: true });
			} catch (error) {
				throw new Error(
					`Failed to clean up temporary SQLite directory "${directory}": ${formatError(error)}`,
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
		"PostgreSQL Prisma fixture generation requires an existing PostgreSQL server.",
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

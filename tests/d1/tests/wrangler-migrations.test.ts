import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
	copyFile,
	cp,
	mkdir,
	mkdtemp,
	readdir,
	readFile,
	rm,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { test } from "vitest";

const fixtureDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = resolve(fixtureDirectory, "..", "..");
const migrationFixtureDirectory = join(fixtureDirectory, "migrations");
const wranglerConfigFixture = join(
	fixtureDirectory,
	"tests",
	"wrangler",
	"wrangler.jsonc",
);
const wranglerEntryPoint = join(
	repositoryRoot,
	"node_modules",
	"wrangler",
	"bin",
	"wrangler.js",
);

interface WranglerResultSet {
	success: unknown;
	results?: unknown[];
}

interface MigrationHistoryRow {
	id: number;
	name: string;
}

interface NamedRow {
	name: string;
}

interface TableInfoRow {
	name: string;
	type: string;
	notnull: number;
}

interface IndexListRow {
	name: string;
}

interface ForeignKeyRow {
	table: string;
	from: string;
	to: string;
	on_delete: string;
	on_update: string;
}

interface PreservedRow {
	post_id: number;
	title: string;
	author_id: number;
	author_name: string;
}

function errorOutput(error: unknown, property: "stdout" | "stderr"): string {
	if (typeof error !== "object" || error === null) {
		return "";
	}

	const value = (error as Record<string, unknown>)[property];

	if (typeof value === "string") {
		return value;
	}

	if (value instanceof Uint8Array) {
		return Buffer.from(value).toString("utf8");
	}

	return value === undefined || value === null ? "" : String(value);
}

function runWrangler(args: readonly string[]): string {
	try {
		return execFileSync(process.execPath, [wranglerEntryPoint, ...args], {
			cwd: repositoryRoot,
			encoding: "utf8",
			env: {
				...process.env,
				CI: "true",
			},
		});
	} catch (error) {
		const stdout = errorOutput(error, "stdout");
		const stderr = errorOutput(error, "stderr");

		throw new Error(
			[
				`Wrangler command failed: ${args.join(" ")}`,
				stdout.trim(),
				stderr.trim(),
			]
				.filter(Boolean)
				.join("\n"),
			{ cause: error },
		);
	}
}

function applyMigrations(
	configPath: string,
	persistenceDirectory: string,
): string {
	return runWrangler([
		"d1",
		"migrations",
		"apply",
		"DB",
		"--local",
		"--persist-to",
		persistenceDirectory,
		"--config",
		configPath,
	]);
}

function parseResultSet(value: unknown): WranglerResultSet {
	if (typeof value !== "object" || value === null) {
		throw new Error(
			`Wrangler result set must be an object: ${JSON.stringify(value)}`,
		);
	}

	const record = value as Record<string, unknown>;

	if (!("success" in record)) {
		throw new Error(
			`Wrangler result set must include success: ${JSON.stringify(value)}`,
		);
	}

	if (record.results !== undefined && !Array.isArray(record.results)) {
		throw new Error(
			`Wrangler result set results must be an array: ${JSON.stringify(value)}`,
		);
	}

	return {
		success: record.success,
		results: record.results as unknown[] | undefined,
	};
}

function executeSql<Row = Record<string, unknown>>(
	configPath: string,
	persistenceDirectory: string,
	command: string,
): Row[] {
	const output = runWrangler([
		"d1",
		"execute",
		"DB",
		"--local",
		"--persist-to",
		persistenceDirectory,
		"--config",
		configPath,
		"--command",
		command,
		"--yes",
		"--json",
	]);
	const parsed: unknown = JSON.parse(output);

	if (!Array.isArray(parsed)) {
		throw new Error("Wrangler JSON output must be an array");
	}

	const resultSets = parsed.map(parseResultSet);

	for (const resultSet of resultSets) {
		assert.equal(
			resultSet.success,
			true,
			`Wrangler SQL execution failed: ${JSON.stringify(resultSet)}`,
		);
	}

	return resultSets.flatMap((resultSet) => (resultSet.results ?? []) as Row[]);
}

async function migrationFixturesInOrder(): Promise<string[]> {
	const entries = await readdir(migrationFixtureDirectory, {
		withFileTypes: true,
	});

	return entries
		.filter((entry) => entry.isDirectory())
		.map((entry) => entry.name)
		.sort((left, right) => left.localeCompare(right));
}

async function stageMigration(
	migrationName: string,
	migrationsDirectory: string,
): Promise<void> {
	await cp(
		join(migrationFixtureDirectory, migrationName),
		join(migrationsDirectory, migrationName),
		{
			recursive: true,
		},
	);
}

test("Wrangler applies nested D1 migrations locally and records their relative paths", async () => {
	const configContents = await readFile(wranglerConfigFixture, "utf8");

	for (const prohibitedSetting of [
		"account_id",
		"api_token",
		"routes",
		"workers_dev",
	]) {
		assert.equal(
			configContents.includes(prohibitedSetting),
			false,
			`local Wrangler fixture must not contain ${prohibitedSetting}`,
		);
	}

	assert.match(configContents, /"migrations_dir"\s*:\s*"migrations"/);
	assert.match(
		configContents,
		/"migrations_pattern"\s*:\s*"migrations\/\*\/migration\.sql"/,
	);

	const migrationNames = await migrationFixturesInOrder();

	assert.deepEqual(
		migrationNames.map((name) => name.slice(name.indexOf("_") + 1)),
		["initial_schema", "require_post_title", "wide_relation_fixture"],
	);

	const [initialMigration, ...remainingMigrations] = migrationNames;

	assert.ok(initialMigration, "an initial migration fixture must exist");

	const temporaryDirectory = await mkdtemp(
		join(tmpdir(), "vitrail-d1-wrangler-"),
	);
	const migrationsDirectory = join(temporaryDirectory, "migrations");
	const persistenceDirectory = join(temporaryDirectory, "state");
	const configPath = join(temporaryDirectory, "wrangler.jsonc");

	try {
		await mkdir(migrationsDirectory, { recursive: true });
		await copyFile(wranglerConfigFixture, configPath);

		await stageMigration(initialMigration, migrationsDirectory);

		const initialApply = applyMigrations(configPath, persistenceDirectory);

		assert.match(
			initialApply,
			new RegExp(`${initialMigration}/migration\\.sql`),
		);

		executeSql(
			configPath,
			persistenceDirectory,
			[
				`INSERT INTO "author" ("id", "name") VALUES (41, 'Preserved Author')`,
				`INSERT INTO "post" ("id", "title", "author_id") VALUES (7, 'Preserved Post', 41)`,
			].join("; "),
		);

		for (const migrationName of remainingMigrations) {
			await stageMigration(migrationName, migrationsDirectory);
		}

		const remainingApply = applyMigrations(configPath, persistenceDirectory);

		for (const migrationName of remainingMigrations) {
			assert.match(
				remainingApply,
				new RegExp(`${migrationName}/migration\\.sql`),
			);
		}

		const repeatedApply = applyMigrations(configPath, persistenceDirectory);

		assert.match(repeatedApply, /No migrations to apply/i);

		const migrationHistory = executeSql<MigrationHistoryRow>(
			configPath,
			persistenceDirectory,
			`SELECT "id", "name" FROM "d1_migrations" ORDER BY "id"`,
		);

		assert.deepEqual(
			migrationHistory.map((migration) => migration.name),
			migrationNames.map((name) => `${name}/migration.sql`),
		);
		assert.deepEqual(
			migrationHistory.map((migration) => migration.id),
			[1, 2, 3],
		);

		const tableNames = executeSql<NamedRow>(
			configPath,
			persistenceDirectory,
			`SELECT "name" FROM "sqlite_master" WHERE "type" = 'table' ORDER BY "name"`,
		).map((table) => table.name);

		for (const expectedTable of [
			"author",
			"d1_migrations",
			"post",
			"scalar_record",
			"wide_child",
			"wide_parent",
		]) {
			assert.ok(
				tableNames.includes(expectedTable),
				`expected local D1 table ${expectedTable}`,
			);
		}

		const postColumns = executeSql<TableInfoRow>(
			configPath,
			persistenceDirectory,
			`PRAGMA table_info("post")`,
		);
		const titleColumn = postColumns.find((column) => column.name === "title");

		assert.ok(titleColumn, "redefined post table must contain title");
		assert.equal(
			titleColumn.notnull,
			1,
			"post.title must be required after the redefinition",
		);

		const postIndexes = executeSql<IndexListRow>(
			configPath,
			persistenceDirectory,
			`PRAGMA index_list("post")`,
		);

		assert.ok(
			postIndexes.some((index) => index.name === "post_author_id_idx"),
			"post author index must survive the redefinition",
		);

		const postForeignKeys = executeSql<ForeignKeyRow>(
			configPath,
			persistenceDirectory,
			`PRAGMA foreign_key_list("post")`,
		);

		assert.equal(postForeignKeys.length, 1);

		const [postForeignKey] = postForeignKeys;

		assert.ok(postForeignKey, "post must retain its author foreign key");
		assert.equal(postForeignKey.table, "author");
		assert.equal(postForeignKey.from, "author_id");
		assert.equal(postForeignKey.to, "id");
		assert.equal(postForeignKey.on_delete, "RESTRICT");
		assert.equal(postForeignKey.on_update, "CASCADE");

		const wideColumns = executeSql<TableInfoRow>(
			configPath,
			persistenceDirectory,
			`PRAGMA table_info("wide_child")`,
		);

		assert.equal(wideColumns.length, 35);
		assert.ok(
			wideColumns.some(
				(column) =>
					column.name === "value_33" &&
					column.type === "BIGINT" &&
					column.notnull === 1,
			),
			"wide child table must contain the final required BIGINT column",
		);

		const preservedRows = executeSql<PreservedRow>(
			configPath,
			persistenceDirectory,
			[
				`SELECT "post"."id" AS "post_id"`,
				`, "post"."title" AS "title"`,
				`, "author"."id" AS "author_id"`,
				`, "author"."name" AS "author_name"`,
				` FROM "post"`,
				` JOIN "author" ON "author"."id" = "post"."author_id"`,
				` WHERE "post"."id" = 7`,
			].join(""),
		);

		assert.deepEqual(preservedRows, [
			{
				post_id: 7,
				title: "Preserved Post",
				author_id: 41,
				author_name: "Preserved Author",
			},
		]);

		assert.deepEqual(
			executeSql<Record<string, unknown>>(
				configPath,
				persistenceDirectory,
				`PRAGMA foreign_key_check`,
			),
			[],
			"the redefinition must retain valid foreign-key references",
		);
	} finally {
		await rm(temporaryDirectory, {
			force: true,
			recursive: true,
		});
	}
});

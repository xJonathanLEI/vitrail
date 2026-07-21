import assert from "node:assert/strict";

import { fetchJson, resetDatabase, test } from "./fixture";

interface CrudResponse {
	ok: boolean;
	inserted: {
		id: string;
		minValue: string;
		maxValue: string;
		active: boolean;
		score: number;
		label: string;
		payload: number[];
		createdAt: string;
		metadata: {
			kind: string;
			nested: {
				enabled: boolean;
				count: number;
			};
		};
		note: string | null;
	};
	queried: {
		id: string;
		minValue: string;
		maxValue: string;
	};
	updatedCount: number;
	deletedCount: number;
}

interface QueryCoverageResponse {
	ok: boolean;
	modelFirstPostId: string;
	nestedAuthorId: string;
	helperAuthorId: string;
	paginatedPostId: string;
	nullLabel: string;
	acceptedBindingCount: number;
	acceptedBindingRows: number;
	rejectedBindingError: string;
	wideParentId: string;
	wideChildId: string;
	wideJsonInsertCount: number;
}

interface DecodeFailureResponse {
	ok: boolean;
	error: string;
}

test("direct D1 operations cover scalar transport, relations, query features, limits, and safe decoding", async ({
	miniflare,
}) => {
	const result = await fetchJson<CrudResponse>(miniflare, "/__test/crud", {
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

	await resetDatabase(miniflare);

	const queryCoverage = await fetchJson<QueryCoverageResponse>(
		miniflare,
		"/__test/query-coverage",
		{
			method: "POST",
		},
	);

	assert.equal(queryCoverage.ok, true);
	assert.equal(queryCoverage.modelFirstPostId, "-9223372036854775808");
	assert.equal(queryCoverage.nestedAuthorId, "9223372036854775806");
	assert.equal(queryCoverage.helperAuthorId, queryCoverage.nestedAuthorId);
	assert.equal(queryCoverage.paginatedPostId, "-9223372036854775807");
	assert.equal(queryCoverage.nullLabel, "charlie-null");
	assert.equal(queryCoverage.acceptedBindingCount, 100);
	assert.equal(queryCoverage.acceptedBindingRows, 3);
	assert.match(queryCoverage.rejectedBindingError, /101 bound parameters/);
	assert.match(queryCoverage.rejectedBindingError, /allowed limit of 100/);
	assert.equal(queryCoverage.wideParentId, "9223372036854775807");
	assert.equal(queryCoverage.wideChildId, "-9223372036854775808");
	assert.equal(queryCoverage.wideJsonInsertCount, 3);

	await resetDatabase(miniflare);

	const decodeFailure = await fetchJson<DecodeFailureResponse>(
		miniflare,
		"/__test/direct-decode-error",
		{
			method: "POST",
		},
	);

	assert.equal(decodeFailure.ok, true);
	assert.match(decodeFailure.error, /failed to decode D1 result/);
	assert.match(decodeFailure.error, /not-an-integer/);
});

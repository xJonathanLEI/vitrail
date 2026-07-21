import assert from "node:assert/strict";

import { fetchJson, test } from "./fixture";

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

test("direct D1 CRUD preserves every supported scalar transport", async ({
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
});

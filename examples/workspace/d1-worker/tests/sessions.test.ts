import assert from "node:assert/strict";

import { fetchJson, test } from "./fixture";

interface SessionResponse {
	ok: boolean;
	insertedId: string;
	initialBookmark: string;
	advancedBookmark: string;
	bookmarkReadNote: string;
	updatedCount: number;
	unconstrainedBookmark: string;
	sequentialReadCount: number;
}

test("explicit D1 sessions preserve sequential consistency and bookmarks", async ({
	miniflare,
}) => {
	const result = await fetchJson<SessionResponse>(
		miniflare,
		"/__test/sessions",
		{
			method: "POST",
		},
	);

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
});

PRAGMA defer_foreign_keys=ON;

-- RedefineTables
CREATE TABLE "new_post" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "title" TEXT NOT NULL,
    "author_id" INTEGER NOT NULL,
    CONSTRAINT "post_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "author" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);
INSERT INTO "new_post" ("author_id", "id", "title") SELECT "author_id", "id", "title" FROM "post";
DROP TABLE "post";
ALTER TABLE "new_post" RENAME TO "post";
CREATE INDEX "post_author_id_idx" ON "post"("author_id");

PRAGMA defer_foreign_keys=OFF;


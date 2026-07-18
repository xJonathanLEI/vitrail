-- CreateTable
CREATE TABLE "comment" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "public_id" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    "post_id" INTEGER NOT NULL,
    CONSTRAINT "comment_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "post_locale" (
    "post_id" INTEGER NOT NULL,
    "locale" TEXT NOT NULL,
    "title" TEXT NOT NULL,

    PRIMARY KEY ("post_id", "locale"),
    CONSTRAINT "post_locale_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateTable
CREATE TABLE "translation_note" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "post_id" INTEGER NOT NULL,
    "locale" TEXT NOT NULL,
    "body" TEXT NOT NULL,
    CONSTRAINT "translation_note_post_id_locale_fkey" FOREIGN KEY ("post_id", "locale") REFERENCES "post_locale" ("post_id", "locale") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- RedefineTables
PRAGMA defer_foreign_keys=ON;
PRAGMA foreign_keys=OFF;
CREATE TABLE "new_post" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "public_id" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "body" TEXT,
    "published" BOOLEAN NOT NULL,
    "author_id" INTEGER NOT NULL,
    "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "score" REAL NOT NULL,
    "updated_at" DATETIME,
    "checksum" BLOB,
    CONSTRAINT "post_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "user" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);
INSERT INTO "new_post" ("author_id", "body", "created_at", "id", "public_id", "published", "title") SELECT "author_id", "body", "created_at", "id", "public_id", "published", "title" FROM "post";
DROP TABLE "post";
ALTER TABLE "new_post" RENAME TO "post";
CREATE UNIQUE INDEX "post_public_id_key" ON "post"("public_id");
CREATE INDEX "post_author_id_idx" ON "post"("author_id");
CREATE INDEX "post_published_created_at_idx" ON "post"("published", "created_at");
PRAGMA foreign_keys=ON;
PRAGMA defer_foreign_keys=OFF;

-- CreateIndex
CREATE UNIQUE INDEX "comment_public_id_key" ON "comment"("public_id");

-- CreateIndex
CREATE INDEX "post_locale_title_locale_idx" ON "post_locale"("title", "locale");

-- CreateIndex
CREATE UNIQUE INDEX "post_locale_post_id_title_key" ON "post_locale"("post_id", "title");


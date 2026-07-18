-- CreateTable
CREATE TABLE "user" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "external_id" TEXT NOT NULL,
    "email" TEXT NOT NULL,
    "name" TEXT NOT NULL,
    "created_at" DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- CreateTable
CREATE TABLE "post" (
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

-- CreateIndex
CREATE UNIQUE INDEX "user_external_id_key" ON "user"("external_id");

-- CreateIndex
CREATE UNIQUE INDEX "user_email_key" ON "user"("email");

-- CreateIndex
CREATE UNIQUE INDEX "post_public_id_key" ON "post"("public_id");

-- CreateIndex
CREATE INDEX "post_author_id_idx" ON "post"("author_id");

-- CreateIndex
CREATE INDEX "post_published_created_at_idx" ON "post"("published", "created_at");

-- CreateIndex
CREATE UNIQUE INDEX "comment_public_id_key" ON "comment"("public_id");

-- CreateIndex
CREATE INDEX "post_locale_title_locale_idx" ON "post_locale"("title", "locale");

-- CreateIndex
CREATE UNIQUE INDEX "post_locale_post_id_title_key" ON "post_locale"("post_id", "title");


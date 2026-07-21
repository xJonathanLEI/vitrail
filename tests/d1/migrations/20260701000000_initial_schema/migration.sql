-- CreateTable
CREATE TABLE "scalar_record" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "min_value" INTEGER NOT NULL,
    "max_value" BIGINT NOT NULL,
    "active" BOOLEAN NOT NULL,
    "score" REAL NOT NULL,
    "label" TEXT NOT NULL,
    "payload" BLOB NOT NULL,
    "created_at" DATETIME NOT NULL,
    "metadata" JSONB NOT NULL,
    "note" TEXT
);

-- CreateTable
CREATE TABLE "author" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "name" TEXT NOT NULL
);

-- CreateTable
CREATE TABLE "post" (
    "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "title" TEXT,
    "author_id" INTEGER NOT NULL,
    CONSTRAINT "post_author_id_fkey" FOREIGN KEY ("author_id") REFERENCES "author" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "scalar_record_label_key" ON "scalar_record"("label");

-- CreateIndex
CREATE INDEX "post_author_id_idx" ON "post"("author_id");


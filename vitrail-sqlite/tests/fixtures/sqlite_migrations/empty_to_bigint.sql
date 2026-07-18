-- CreateTable
CREATE TABLE "account" (
    "id" BIGINT NOT NULL PRIMARY KEY,
    "external_ref" BIGINT NOT NULL,
    "credit_limit" BIGINT NOT NULL
);

-- CreateTable
CREATE TABLE "invoice" (
    "id" BIGINT NOT NULL PRIMARY KEY,
    "account_id" BIGINT NOT NULL,
    "amount_cents" BIGINT NOT NULL,
    "settled_at" BIGINT,
    CONSTRAINT "invoice_account_id_fkey" FOREIGN KEY ("account_id") REFERENCES "account" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);

-- CreateIndex
CREATE UNIQUE INDEX "account_external_ref_key" ON "account"("external_ref");

-- CreateIndex
CREATE INDEX "invoice_account_id_idx" ON "invoice"("account_id");

-- CreateIndex
CREATE UNIQUE INDEX "invoice_account_id_amount_cents_key" ON "invoice"("account_id", "amount_cents");


-- CreateTable
CREATE TABLE "wide_parent" (
    "id" BIGINT NOT NULL PRIMARY KEY
);

-- CreateTable
CREATE TABLE "wide_child" (
    "id" BIGINT NOT NULL PRIMARY KEY,
    "parent_id" BIGINT NOT NULL,
    "value_01" BIGINT NOT NULL,
    "value_02" BIGINT NOT NULL,
    "value_03" BIGINT NOT NULL,
    "value_04" BIGINT NOT NULL,
    "value_05" BIGINT NOT NULL,
    "value_06" BIGINT NOT NULL,
    "value_07" BIGINT NOT NULL,
    "value_08" BIGINT NOT NULL,
    "value_09" BIGINT NOT NULL,
    "value_10" BIGINT NOT NULL,
    "value_11" BIGINT NOT NULL,
    "value_12" BIGINT NOT NULL,
    "value_13" BIGINT NOT NULL,
    "value_14" BIGINT NOT NULL,
    "value_15" BIGINT NOT NULL,
    "value_16" BIGINT NOT NULL,
    "value_17" BIGINT NOT NULL,
    "value_18" BIGINT NOT NULL,
    "value_19" BIGINT NOT NULL,
    "value_20" BIGINT NOT NULL,
    "value_21" BIGINT NOT NULL,
    "value_22" BIGINT NOT NULL,
    "value_23" BIGINT NOT NULL,
    "value_24" BIGINT NOT NULL,
    "value_25" BIGINT NOT NULL,
    "value_26" BIGINT NOT NULL,
    "value_27" BIGINT NOT NULL,
    "value_28" BIGINT NOT NULL,
    "value_29" BIGINT NOT NULL,
    "value_30" BIGINT NOT NULL,
    "value_31" BIGINT NOT NULL,
    "value_32" BIGINT NOT NULL,
    "value_33" BIGINT NOT NULL,
    CONSTRAINT "wide_child_parent_id_fkey" FOREIGN KEY ("parent_id") REFERENCES "wide_parent" ("id") ON DELETE RESTRICT ON UPDATE CASCADE
);


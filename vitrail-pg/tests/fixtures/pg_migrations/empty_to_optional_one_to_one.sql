-- CreateTable
CREATE TABLE "user" (
    "id" SERIAL NOT NULL,

    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "profile" (
    "id" SERIAL NOT NULL,
    "user_id" INTEGER,

    CONSTRAINT "profile_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "profile_user_id_key" ON "profile"("user_id");

-- AddForeignKey
ALTER TABLE "profile" ADD CONSTRAINT "profile_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "user"("id") ON DELETE SET NULL ON UPDATE CASCADE;


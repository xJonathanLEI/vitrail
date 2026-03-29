-- AlterTable
ALTER TABLE "post" ADD COLUMN     "updated_at" TIMESTAMP(3);

-- CreateTable
CREATE TABLE "comment" (
    "id" SERIAL NOT NULL,
    "body" TEXT NOT NULL,
    "post_id" INTEGER NOT NULL,

    CONSTRAINT "comment_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "post_locale" (
    "post_id" INTEGER NOT NULL,
    "locale" TEXT NOT NULL,
    "title" TEXT NOT NULL,

    CONSTRAINT "post_locale_pkey" PRIMARY KEY ("post_id","locale")
);

-- CreateTable
CREATE TABLE "translation_note" (
    "id" SERIAL NOT NULL,
    "post_id" INTEGER NOT NULL,
    "locale" TEXT NOT NULL,
    "body" TEXT NOT NULL,

    CONSTRAINT "translation_note_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "comment" ADD CONSTRAINT "comment_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "post_locale" ADD CONSTRAINT "post_locale_post_id_fkey" FOREIGN KEY ("post_id") REFERENCES "post"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "translation_note" ADD CONSTRAINT "translation_note_post_id_locale_fkey" FOREIGN KEY ("post_id", "locale") REFERENCES "post_locale"("post_id", "locale") ON DELETE RESTRICT ON UPDATE CASCADE;


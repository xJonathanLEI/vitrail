export default {
  schema: "./base.prisma",
  datasource: {
    url: process.env.DATABASE_URL ?? "",
  },
};

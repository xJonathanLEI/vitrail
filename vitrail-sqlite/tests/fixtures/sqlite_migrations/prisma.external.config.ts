export default {
  schema: "./base.prisma",
  datasource: {
    url: process.env.DATABASE_URL ?? "",
  },
  experimental: {
    externalTables: true,
  },
  tables: {
    external: ["external_audit_log"],
  },
};

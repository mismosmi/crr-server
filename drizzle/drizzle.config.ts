import { Config } from "drizzle-kit";

export default {
  schema: "./schema.ts",
  out: "./migrations",
} satisfies Config;

import { sqliteTable, text } from "drizzle-orm/sqlite-core";

export const table = sqliteTable("test", {
  id: text("id").primaryKey(),
  val: text("val"),
});

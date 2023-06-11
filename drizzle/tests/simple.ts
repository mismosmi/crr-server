import { createClient, createMigratorClient } from "../src";
import { drizzle } from "drizzle-orm/sqlite-proxy";
import { migrate } from "drizzle-orm/sqlite-proxy/migrator";
import { table } from "../schema";

describe("Basic Functionality", () => {
  const url = "http://127.0.0.1:6839/db/test-basic";
  const db = drizzle(createClient(url));

  it("runs migrations", async () => {
    await migrate(db, createMigratorClient(url), {
      migrationsFolder: "migrations",
    });

    await db
      .insert(table)
      .values({ id: "first", val: "This is the first row" })
      .onConflictDoNothing()
      .run();

    const { val } = await db.select({ val: table.val }).from(table).get();

    expect(val).toStrictEqual(["This is the first row"]);
  });
});

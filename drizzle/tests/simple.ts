import { createClient, createMigratorClient } from "../src";
import { drizzle } from "drizzle-orm/sqlite-proxy";
import { migrate } from "drizzle-orm/sqlite-proxy/migrator";
import { table } from "../schema";

describe("Basic Functionality", () => {
  const url = `${process.env.CRR_SERVER_URL}/db/test-basic`;
  const db = drizzle(createClient(url, process.env.CRR_SERVER_TOKEN!));

  it("runs migrations", async () => {
    await migrate(
      db,
      createMigratorClient(url, process.env.CRR_SERVER_TOKEN!),
      {
        migrationsFolder: "migrations",
      }
    );

    await db
      .insert(table)
      .values({ id: "first", val: "This is the first row" })
      .onConflictDoNothing()
      .run();

    const { val } = await db.select({ val: table.val }).from(table).get();

    expect(val).toStrictEqual(["This is the first row"]);
  });
});

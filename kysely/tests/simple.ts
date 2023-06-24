import { Kysely, Migrator } from "kysely";
import { CRRMigrationProvider } from "../src/migrations";
import { CRRDialect } from "../src/dialect";

describe("basic functionality", () => {
  interface Foo {
    id: string;
    bar: string;
  }

  interface Database {
    foo: Foo;
  }
  it("runs basic migrations", async () => {
    const db = new Kysely<Database>({
      dialect: new CRRDialect(
        `${process.env.CRR_SERVER_URL}/db/test-simple`,
        process.env.CRR_SERVER_TOKEN!
      ),
    });
    const migrator = new Migrator({
      db,
      provider: new CRRMigrationProvider({
        "001_foo": {
          async up(db) {
            await db.schema
              .createTable("foo")
              .addColumn("id", "text", (col) => col.primaryKey())
              .addColumn("bar", "text")
              .execute();
          },
        },
      }),
    });

    await migrator.migrateToLatest();
  });
});

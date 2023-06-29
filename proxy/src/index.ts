import initWasm, { DB, SQLite3 } from "@vlcn.io/crsqlite-wasm";
import { Changeset } from "./changeset";
import { Migration } from "./migration";

export class CRRClientProxy {
  private sqlite: Promise<SQLite3>;
  constructor(
    getWasmUrl: () => string,
    private serverUrl: string,
    private dbName: string,
    private token?: string
  ) {
    this.sqlite = initWasm(getWasmUrl);
  }

  private async db(): Promise<DB> {
    const sqlite = await this.sqlite;
    return sqlite.open(`${this.dbName}.sqlite3`);
  }

  async install() {
    const db = await this.db();

    await db.execO(
      "CREATE TABLE IF NOT EXISTS crr_client_migrations (version INTEGER PRIMARY KEY, sql TEXT NOT NULL"
    );

    await db.close();
  }

  private async fetch(path: string) {
    const url = new URL(path, this.serverUrl);

    const headers = new Headers({
      "Content-Type": "application/json",
      Accept: "application/json",
    });

    if (this.token) {
      headers.append("Authorization", `Bearer ${this.token}`);
    }

    const res = await fetch(url, { headers });

    if (!res.ok) {
      const { message } = await res.json();
      throw new Error(message);
    }

    return res.json();
  }

  async activate() {
    const db = await this.db();

    const [[schemaVersion]] = await db.execA(
      "SELECT version FROM crr_client_migrations ORDER BY version LIMIT 1"
    );

    const [[dbVersion, siteId]] = await db.execA(
      "SELECT crsql_dbversion(), crsql_siteid()"
    );

    console.debug(
      "Start Sync for",
      this.dbName,
      "SchemaVersion",
      schemaVersion,
      "DbVersion",
      dbVersion,
      "SiteID",
      siteId
    );

    const url = new URL(`/db/${this.dbName}/changes`, this.serverUrl);
    url.searchParams.append("site_id", btoa(siteId));
    url.searchParams.append("db_version", dbVersion);
    url.searchParams.append("schema_version", schemaVersion);

    const { signedUrl } = await this.fetch(
      `/auth/signed-url?url=${encodeURIComponent(url.toString())}`
    );

    const eventSource = new EventSource(signedUrl);

    eventSource.addEventListener("change", (event: MessageEvent) => {
      const data = JSON.parse(event.data);
      const changeset: Changeset = {
        table: data.table,
        pk: data.pk,
        cid: data.cid,
        val: data.val,
        col_version: data.col_version,
        db_version: data.db_version,
        site_id: data.site_id,
      };

      console.log(changeset);
    });

    eventSource.addEventListener("migration", (event: MessageEvent) => {
      const data = JSON.parse(event.data);

      const migration: Migration = {
        version: data.version,
        sql: data.sql,
      };

      console.log(migration);
    });

    eventSource.addEventListener("error", (event: MessageEvent) => {
      const { message } = JSON.parse(event.data);

      throw new Error(message);
    });
  }

  shouldHandle(url: URL) {
    if (url.host !== this.serverUrl) {
      return false;
    }

    const [db, dbName, run] = url.pathname.split("/");

    if (db !== "db") {
      return false;
    }

    if (dbName !== this.dbName) {
      return false;
    }

    if (run !== "run") {
      return false;
    }

    return true;
  }

  async respondTo(req: Request) {
    const { sql, params } = await req.json();

    const db = await this.db();

    const rows = await db.execA(sql, params);
    return new Response(
      JSON.stringify({
        rows,
      }),
      { headers: { "Content-Type": "application/json" } }
    );
  }
}

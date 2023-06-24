import {
  CompiledQuery,
  DatabaseConnection,
  DatabaseIntrospector,
  Dialect,
  Driver,
  Kysely,
  QueryCompiler,
  QueryResult,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
  TransactionSettings,
} from "kysely";

class CRRConnection implements DatabaseConnection {
  constructor(private url: string, private token: string) {}

  async executeQuery<R>(
    compiledQuery: CompiledQuery<unknown>
  ): Promise<QueryResult<R>> {
    const result = await fetch(`${this.url}/run`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Cookie: `CRR_TOKEN=${this.token}`,
      },
      body: JSON.stringify({
        sql: compiledQuery.sql,
        params: compiledQuery.parameters,
        method: "all",
      }),
    });
    console.debug(
      "execute",
      compiledQuery.sql,
      compiledQuery.parameters,
      await result.json()
    );

    if (!result.ok) {
      const { message } = await result.json();
      throw new Error(`CRR-Server Error: ${message}`);
    }

    return result.json();
  }

  streamQuery<R>(
    compiledQuery: CompiledQuery<unknown>,
    chunkSize?: number | undefined
  ): AsyncIterableIterator<QueryResult<R>> {
    throw new Error("Unimplemented");
  }
}

class CRRDriver implements Driver {
  constructor(private url: string, private token: string) {}

  async init(): Promise<void> {}
  async acquireConnection(): Promise<DatabaseConnection> {
    return new CRRConnection(this.url, this.token);
  }
  async beginTransaction(
    connection: DatabaseConnection,
    settings: TransactionSettings
  ): Promise<void> {
    throw new Error("Unimplemented");
  }
  async commitTransaction(connection: DatabaseConnection): Promise<void> {
    throw new Error("Unimplemented");
  }
  async rollbackTransaction(connection: DatabaseConnection): Promise<void> {
    throw new Error("Unimplemented");
  }
  async destroy(): Promise<void> {}
  async releaseConnection(connection: DatabaseConnection): Promise<void> {}
}

export class CRRDialect implements Dialect {
  constructor(private url: string, private token: string) {}

  createAdapter() {
    return new SqliteAdapter();
  }
  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new SqliteIntrospector(db);
  }
  createQueryCompiler(): QueryCompiler {
    return new SqliteQueryCompiler();
  }
  createDriver(): Driver {
    return new CRRDriver(this.url, this.token);
  }
}

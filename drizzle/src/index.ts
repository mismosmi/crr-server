export function createClient(url: string, token: string) {
  return async function CRRClient(
    sql: string,
    params: any,
    method: "run" | "all" | "values" | "get"
  ) {
    let res: Response;
    try {
      res = await fetch(`${url}/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Cookie: `CRR_TOKEN=${token}`,
        },
        body: JSON.stringify({
          sql,
          params,
          method,
        }),
      });
    } catch (error: unknown) {
      console.error(
        `Connection to CRR-Server failed: ${getErrorMessage(error)}`
      );
      return {
        rows: [],
      };
    }

    if (!res.ok) {
      try {
        const { message } = await res.json();
        console.error(`CRR-Server Error: ${message}`);
      } catch (error: unknown) {
        console.error(
          `CRR-Server Error: Failed to parse Error Message: ${getErrorMessage(
            error
          )}`
        );
      }

      return {
        rows: [],
      };
    }

    try {
      return await res.json();
    } catch (error: unknown) {
      console.error(
        `Failed to parse response from CRR-Server: ${getErrorMessage(error)}`
      );
    }
  };
}

export function createMigratorClient(url: string, token: string) {
  return async function CRRMigratorClient(queries: string[]) {
    const res = await fetch(`${url}/migrate`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
        Cookie: `CRR_TOKEN=${token}`,
      },
      body: JSON.stringify({
        queries,
      }),
    });

    if (!res.ok) {
      const { message } = await res.json();
      throw new Error(`Failed to apply migrations: ${message}`);
    }
  };
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  } else if (typeof error === "string") {
    return error;
  } else {
    return "Unexpected Error";
  }
}

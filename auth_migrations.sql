CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    otp TEXT
);

CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER,
    role_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (role_id) REFERENCES roles (id),
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS permissions (
    role_id INTEGER,
    database_name TEXT NOT NULL,
    table_name TEXT,
    pread BOOLEAN NOT NULL DEFAULT FALSE,
    pinsert BOOLEAN NOT NULL DEFAULT FALSE,
    pupdate BOOLEAN NOT NULL DEFAULT FALSE,
    pdelete BOOLEAN NOT NULL DEFAULT FALSE,
    pfull BOOLEAN NOT NULL DEFAULT FALSE,
    FOREIGN KEY (role_id) REFERENCES roles (id),
    PRIMARY KEY (role_id, database_name, table_name)
);

CREATE INDEX IF NOT EXISTS permissions_by_role_and_db ON permissions (role_id, database_name);

CREATE TABLE IF NOT EXISTS tokens (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    token TEXT UNIQUE NOT NULL,
    expires TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    otp TEXT
);

CREATE TABLE IF NOT EXISTS roles (id INTEGER PRIMARY KEY AUTOINCREMENT);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER,
    role_id INTEGER,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (role_id) REFERENCES roles (id),
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS database_owners (
    user_id INTEGER,
    database_name TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id),
    PRIMARY KEY (user_id, database_name)
);

CREATE TABLE IF NOT EXISTS table_permissions (
    role_id INTEGER,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    pread BOOLEAN NOT NULL,
    pupdate BOOLEAN NOT NULL,
    pinsert BOOLEAN NOT NULL,
    pdelete BOOLEAN NOT NULL,
    FOREIGN KEY (role_id) REFERENCES roles (id),
    PRIMARY KEY (role_id, database_name, table_name)
);

CREATE TABLE IF NOT EXISTS tokens (
    user_id INTEGER,
    token TEXT PRIMARY KEY,
    expires TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id)
);
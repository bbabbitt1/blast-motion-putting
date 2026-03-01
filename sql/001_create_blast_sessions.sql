
CREATE SCHEMA IF NOT EXISTS src;

CREATE TABLE IF NOT EXISTS src.blast_sessions (
    blast_session_id    INT             PRIMARY KEY,
    session_type_id     INT             NOT NULL,
    session_name        VARCHAR(255)    NOT NULL,
    created_at          TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);
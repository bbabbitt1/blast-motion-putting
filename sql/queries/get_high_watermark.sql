SELECT
    COALESCE(max(blast_session_id), 0)
FROM src.blast_sessions;

CREATE TABLE IF NOT EXISTS src.blast_putts (
    blast_id                UUID            PRIMARY KEY,
    session_id              INT             NOT NULL REFERENCES src.blast_sessions(blast_session_id),
    user_id                 INT             NOT NULL,
    putt_date               TIMESTAMP       NOT NULL,
    equipment               VARCHAR(255),
    handedness              VARCHAR(50),
    is_air_swing            BOOLEAN         DEFAULT FALSE,
    back_stroke_time        NUMERIC(6,2),
    forward_stroke_time     NUMERIC(6,2),
    total_stroke_time       NUMERIC(6,2),
    tempo                   NUMERIC(6,2),
    impact_stroke_speed     NUMERIC(6,2),
    back_stroke_length      NUMERIC(6,2),
    loft_change             NUMERIC(6,2),
    backstroke_rotation     NUMERIC(6,2),
    forward_stroke_rotation NUMERIC(6,2),
    face_angle_at_impact    NUMERIC(6,2),
    lie_change              NUMERIC(6,2),
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_blast_putts_session_id 
    ON src.blast_putts(session_id);

CREATE INDEX idx_blast_putts_putt_date 
    ON src.blast_putts(putt_date);
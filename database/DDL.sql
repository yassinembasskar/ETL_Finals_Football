-- ============================================================================
-- PostgreSQL 16 schema for the football ETL pipeline
-- Generated from transform.py's table-producing functions.
-- Run this once to create all tables before loading the parquet files.
-- ============================================================================

BEGIN;

-- ----------------------------------------------------------------------------
-- team: one row per team, deduplicated by team_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS team (
    team_id     BIGINT PRIMARY KEY,
    team_name   TEXT
);

-- ----------------------------------------------------------------------------
-- players: identity table, deduplicated by IdPlayer, no match-specific data
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    id_player     BIGINT PRIMARY KEY,
    name          TEXT,
    country       TEXT,
    market_value  NUMERIC,
    date_of_birth TIMESTAMP,
    height        NUMERIC
);

-- ----------------------------------------------------------------------------
-- match: one row per event_id
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match (
    event_id            BIGINT PRIMARY KEY,
    competition         TEXT,
    kickoff             TEXT,           
    home_team_id        BIGINT REFERENCES team(team_id),
    away_team_id        BIGINT REFERENCES team(team_id),
    home_score          NUMERIC,
    away_score          NUMERIC,
    slug                TEXT,
    custom_id           TEXT,
    sofascore_link      TEXT,
    full_highlight_url  TEXT
);

-- ----------------------------------------------------------------------------
-- match_team: one row per team per match (isHome, score, formation)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_team (
    event_id    BIGINT NOT NULL REFERENCES match(event_id),
    team_id     BIGINT NOT NULL REFERENCES team(team_id),
    is_home     BOOLEAN,
    score       NUMERIC,
    formation   TEXT,
    PRIMARY KEY (event_id, team_id)
);

-- ----------------------------------------------------------------------------
-- match_team_stats: long format, one row per (event_id, team_id, stat_name)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_team_stats (
    event_id    BIGINT NOT NULL REFERENCES match(event_id),
    team_id     BIGINT NOT NULL REFERENCES team(team_id),
    stat_name   TEXT NOT NULL,
    stat_value  NUMERIC,
    PRIMARY KEY (event_id, team_id, stat_name)
);

-- ----------------------------------------------------------------------------
-- match_players: one row per player per match (meta fields + average position)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_players (
    event_id        BIGINT NOT NULL REFERENCES match(event_id),
    id_player       BIGINT NOT NULL REFERENCES players(id_player),
    team_id         BIGINT REFERENCES team(team_id),
    jersey_number   TEXT,
    position        TEXT,
    substitute      BOOLEAN,
    captain         BOOLEAN,
    average_x       NUMERIC,
    average_y       NUMERIC,
    PRIMARY KEY (event_id, id_player)
);

-- ----------------------------------------------------------------------------
-- match_player_stats: long format, one row per (event_id, team_id, player_id, stat_label)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_player_stats (
    event_id    BIGINT NOT NULL REFERENCES match(event_id),
    team_id     BIGINT REFERENCES team(team_id),
    player_id   BIGINT NOT NULL REFERENCES players(id_player),
    stat_label  TEXT NOT NULL,
    stat_value  NUMERIC,
    PRIMARY KEY (event_id, team_id, player_id, stat_label)
);

-- ----------------------------------------------------------------------------
-- goals: one row per goal incident
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS goals (
    goal_id         BIGINT PRIMARY KEY,
    event_id        BIGINT NOT NULL REFERENCES match(event_id),
    team_id         BIGINT REFERENCES team(team_id),
    is_home         BOOLEAN,
    home_score      NUMERIC,
    away_score      NUMERIC,
    time            NUMERIC,
    added_time      NUMERIC,
    has_assist      BOOLEAN,
    player_id       BIGINT REFERENCES players(id_player),
    assist1_id      BIGINT REFERENCES players(id_player)
);

-- ----------------------------------------------------------------------------
-- cards: one row per card incident
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS cards (
    card_id          BIGINT PRIMARY KEY,
    event_id         BIGINT NOT NULL REFERENCES match(event_id),
    team_id          BIGINT REFERENCES team(team_id),
    is_home          BOOLEAN,
    incident_class   TEXT,
    time             NUMERIC,
    added_time       NUMERIC,
    player_id        BIGINT REFERENCES players(id_player)
);

-- ----------------------------------------------------------------------------
-- substitutions: one row per substitution incident
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS substitutions (
    sub_id          BIGINT PRIMARY KEY,
    event_id        BIGINT NOT NULL REFERENCES match(event_id),
    team_id         BIGINT REFERENCES team(team_id),
    is_home         BOOLEAN,
    injury          BOOLEAN,
    time            NUMERIC,
    added_time      NUMERIC,
    player_in_id    BIGINT REFERENCES players(id_player),
    player_out_id   BIGINT REFERENCES players(id_player)
);

-- ----------------------------------------------------------------------------
-- passing_network: one row per action in a goal's build-up sequence
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS passing_network (
    event_id              BIGINT NOT NULL REFERENCES match(event_id),
    goal_id               BIGINT NOT NULL REFERENCES goals(goal_id),
    team_id               BIGINT REFERENCES team(team_id),
    player_id             BIGINT REFERENCES players(id_player),
    type                  TEXT,
    "order"               INTEGER,
    player_coordinates    JSONB,
    action_coordinates    JSONB,
    has_action_coordinates BOOLEAN,
    PRIMARY KEY (goal_id, "order")
);

-- ----------------------------------------------------------------------------
-- highlights: per-match highlight clips (filtered to key_subtitles)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS highlights (
    event_id              BIGINT NOT NULL REFERENCES match(event_id),
    title                 TEXT,
    subtitle              TEXT,
    url                   TEXT,
    created_at_timestamp  BIGINT
);

-- ----------------------------------------------------------------------------
-- shotmaps: one row per shot
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS shotmaps (
    event_id              BIGINT NOT NULL REFERENCES match(event_id),
    player_id             BIGINT REFERENCES players(id_player),
    team_id               BIGINT REFERENCES team(team_id),
    shot_type             TEXT,
    situation             TEXT,
    player_coordinates    JSONB,
    body_part             TEXT,
    goal_mouth_location   TEXT,
    goal_mouth_coordinates JSONB,
    block_coordinates     JSONB,
    xg                    NUMERIC,
    xgot                  NUMERIC,
    goalkeeper_id         BIGINT REFERENCES players(id_player),
    time                  NUMERIC,
    added_time            NUMERIC
);

-- ----------------------------------------------------------------------------
-- Helpful indexes for common query patterns (event_id / player_id lookups)
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_match_team_event ON match_team(event_id);
CREATE INDEX IF NOT EXISTS idx_match_team_stats_event ON match_team_stats(event_id);
CREATE INDEX IF NOT EXISTS idx_match_players_event ON match_players(event_id);
CREATE INDEX IF NOT EXISTS idx_match_player_stats_event ON match_player_stats(event_id);
CREATE INDEX IF NOT EXISTS idx_match_player_stats_player ON match_player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_goals_event ON goals(event_id);
CREATE INDEX IF NOT EXISTS idx_cards_event ON cards(event_id);
CREATE INDEX IF NOT EXISTS idx_substitutions_event ON substitutions(event_id);
CREATE INDEX IF NOT EXISTS idx_passing_network_event ON passing_network(event_id);
CREATE INDEX IF NOT EXISTS idx_highlights_event ON highlights(event_id);
CREATE INDEX IF NOT EXISTS idx_shotmaps_event ON shotmaps(event_id);
CREATE INDEX IF NOT EXISTS idx_shotmaps_player ON shotmaps(player_id);

COMMIT;
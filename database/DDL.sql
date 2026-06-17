CREATE TABLE IF NOT EXISTS teams (
    id          SERIAL PRIMARY KEY,
    team_name   VARCHAR(150),
    logo_url    VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS players (
    id          SERIAL PRIMARY KEY,
    player_name VARCHAR(150),
    nationality VARCHAR(100),
    position    VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS matches (
    id              SERIAL PRIMARY KEY,
    match_date      DATE,
    competition     VARCHAR(150),
    home_team_id    INTEGER REFERENCES teams(id),
    away_team_id    INTEGER REFERENCES teams(id),
    home_score      INTEGER,
    away_score      INTEGER,
    enjoyment_score FLOAT,
    etl_status      VARCHAR(20) DEFAULT 'PENDING',
    last_modified   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS match_teams (
    id              SERIAL PRIMARY KEY,
    match_id        INTEGER REFERENCES matches(id),
    team_id         INTEGER REFERENCES teams(id),
    possession      FLOAT,
    shots           INTEGER,
    shots_on_target INTEGER,
    xg              FLOAT,
    passes          INTEGER,
    saves           INTEGER
);

CREATE TABLE IF NOT EXISTS match_players (
    id              SERIAL PRIMARY KEY,
    match_id        INTEGER REFERENCES matches(id),
    player_id       INTEGER REFERENCES players(id),
    team_id         INTEGER REFERENCES teams(id),
    minutes_played  INTEGER,
    goals           INTEGER,
    assists         INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    rating          FLOAT,
    starting_bench  VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS events (
    id          SERIAL PRIMARY KEY,
    match_id    INTEGER REFERENCES matches(id),
    player_id   INTEGER REFERENCES players(id),
    team_id     INTEGER REFERENCES teams(id),
    minute      INTEGER,
    event_type  VARCHAR(50),
    x_coord     FLOAT,
    y_coord     FLOAT
);

CREATE TABLE IF NOT EXISTS narratives (
    id              SERIAL PRIMARY KEY,
    match_id        INTEGER REFERENCES matches(id),
    generated_text  TEXT,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(etl_status);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_events_match ON events(match_id);
CREATE INDEX IF NOT EXISTS idx_match_players_match ON match_players(match_id);

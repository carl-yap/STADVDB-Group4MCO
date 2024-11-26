-- Update Node Database Initialization
CREATE TABLE steam_games (
    game_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    developer VARCHAR(255),
    publisher VARCHAR(255),
    price NUMERIC(10, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Initially empty, will be used for update operations

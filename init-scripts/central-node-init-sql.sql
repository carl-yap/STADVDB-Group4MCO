-- Central Node Database Initialization
CREATE TABLE steam_games (
    game_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    developer VARCHAR(255),
    publisher VARCHAR(255),
    price NUMERIC(10, 2),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed with initial data
INSERT INTO steam_games (title, developer, publisher, price) VALUES 
('Counter-Strike: Global Offensive', 'Valve', 'Valve', 14.99),
('Dota 2', 'Valve', 'Valve', 0.00),
('PUBG: BATTLEGROUNDS', 'KRAFTON, Inc.', 'KRAFTON, Inc.', 29.99),
('Lost Ark', 'Smilegate RPG', 'Amazon Games', 0.00),
('Apex Legends', 'Respawn Entertainment', 'Electronic Arts', 0.00);

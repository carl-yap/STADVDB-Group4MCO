# STADVDB-Group4MCO
Online repository for Major Course Outputs of Group 4 [STADVDB S16, AY2024-2025]

# Distributed Database System for Steam Games

## Project Setup

### Prerequisites
- Docker
- Docker Compose

### Getting Started

1. Clone the repository
2. Navigate to the project directory
3. Run the following command to start the distributed database:
   ```
   docker-compose up -d
   ```

### Database Connections
- Central Node: 
  - Port: 5432
  - Database: steam_games_central
- Update Node: 
  - Port: 5433
  - Database: steam_games_update
- Replica Node: 
  - Port: 5434
  - Database: steam_games_replica

### Connection Credentials
- Username: admin
- Password: distributed_db_password

### Next Steps
1. Implement replication mechanism
2. Develop concurrency control
3. Create crash and recovery simulation scripts

## Technical Considerations
- Three-node PostgreSQL cluster
- Bridge network for inter-node communication
- Initialized with Steam Games dataset
- Supports concurrent transactions
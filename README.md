# STADVDB-Group4MCO
Online repository for Major Course Outputs of Group 4 [STADVDB S16, AY2024-2025]

MCO1: Data Warehouse with ETL Script for Steam Games
MCO2: Distributed Database System for Steam Games

## Project Setup

### Prerequisites
- Docker, Docker Compose
- pip install flask, flask_cors, psycopg2

### Getting Started

1. Clone the repository
2. Navigate to the project directory
3. Run the following command to start the distributed database:
   ```
   docker-compose up -d
   ```
4. If not yet installed, run pip to meet the prerequisite Python libraries.
5. Run `python "STADVDB MCO2 Group 5 Code.py"`
6. Open the web application from the link provided by Flask.

### Database Connections
**All dbs are in the localhost server**
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
- Password: we<3stadvdb

### Technical Considerations
- Three-node PostgreSQL cluster
- Bridge network for inter-node communication
- Initialized with Steam Games dataset
- Supports concurrent transactions
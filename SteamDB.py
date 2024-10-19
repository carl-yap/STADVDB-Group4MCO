"""
This file contains the SteamDB class. It will setup the data warehouse schema on an empty MySQL database.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from mysql.connector import Error


class SteamDB:
    def __init__(self, db_config, csv_file="data/games_fixed.csv"):
        self.csv_file = csv_file
        self.db_config = db_config
        self.df = None
        self.engine = None
        
        # Internal Setup
        self.load_csv()
        self.create_connection()

    def load_csv(self):
        # Load CSV into a DataFrame
        try:
            self.df = pd.read_csv(self.csv_file)

            # Change the column names to match the database columns
            new_column_names = [
                'gameID', 'name', 'releaseDate', 'NewEstimatedOwners',
                'NewPeakCCU', 'requiredAge', 'NewPrice', 'NewDiscountDLCCount',
                'aboutTheGame', 'supportedLanguages', 'fullAudioLanguages',
                'NewReviews', 'headerImageHREF', 'websiteURL', 'supportURL',
                'supportEmail', 'windowsSupport', 'macSupport', 'linuxSupport',
                'NewMetacriticScore', 'NewMetacriticUrl', 'NewUserScore',
                'NewPositive', 'NewNegative', 'NewScoreRank', 'NewAchievements',
                'NewRecommendations', 'NewNotes', 'NewAveragePlaytimeForever',
                'NewAveragePlaytimeTwoWeeks', 'NewMedianPlaytimeForever',
                'NewMedianPlaytimeTwoWeeks', 'NewDevelopers', 'NewPublishers',
                'categories', 'genres', 'tags', 'NewScreenshots',
                'NewMovies'
            ]
            rename_mapping = dict(zip(self.df.columns, new_column_names))
            self.df.rename(columns=rename_mapping, inplace=True)

            # Change format from "MMMM DD, YYYY" to MM-DD-YYYY for a DATE type
            self.df['releaseDate'] = pd.to_datetime(self.df['releaseDate'], format='mixed', dayfirst=True).dt.strftime('%m-%d-%Y')

            print("CSV loaded into DataFrame.")
        except Error as e:
            print(f"Failed to load CSV: {e}")

    def create_connection(self):
        # Create a connection to the MySQL server
        connection_string = f"mysql+mysqlconnector://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}"
        self.engine = create_engine(connection_string)
        print("Connected to MySQL server.")

        with self.engine.connect() as connection:
            connection.execute(text(f"DROP DATABASE {self.db_config['database']}"))
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.db_config['database']}"))

        # Now reconnect to the specific database
        self.engine = create_engine(f"{connection_string}/{self.db_config['database']}")
        print(f"Connected to database '{self.db_config['database']}'.")

    def create_steam_db(self):
        """ 
            Create our group's database along with the dimension and fact tables 
        """
        try:
            with open("queries/db_builder.txt", 'r') as file:
                sql_queries = file.read()
                queries = [query.strip() for query in sql_queries.strip().split(';') if query.strip()]

                with self.engine.connect() as connection:
                    for query in queries:
                        if query:  # Ensure query is not empty
                            connection.execute(text(query))
                print("SQL query executed successfully!")
        except FileNotFoundError:
            print(f"Error: The file 'queries/db_builder.txt' was not found.")
        except Error as e:
            print(f"Failed to execute SQL query: {e}")

    # Populate the DB using `games_fixed.csv`
    def populate_steam_db(self):
        curr_table = "dim_game"
        try:
            dim_game_attr=['gameID', 'name', 'releaseDate', 'requiredAge', 'aboutTheGame',
                           'websiteURL', 'supportURL', 'supportEmail', 'supportedLanguages', 'fullAudioLanguages', 'headerImageHREF',
                           'categories', 'tags', 'genres']
            dim_game_df = self.df[
                (self.df['name'].notnull()) & (self.df['name'] != '') # Check for non-null and non-empty name
            ]
            dim_game_df = dim_game_df.drop_duplicates(subset=['gameID', 'name']) 
            self.insert_to_mysql(dim_game_df[dim_game_attr], "dim_game")

        except Error as e:
            print(f"Failed to populate table '{curr_table}': {e}")

    def execute_sql(self, sql_query):
        with self.engine.connect() as connection:
            connection.execute(text(sql_query))

    def insert_to_mysql(self, df: pd.DataFrame, table_name):
        with self.engine.connect() as connection:
            # Begin a transaction
            transaction = connection.begin()
            try:
                # Write the DataFrame to a SQL table
                df.to_sql(table_name, 
                          con=connection, 
                          if_exists='replace', 
                          index=False)
                
                # Commit the transaction if successful
                transaction.commit()
                print("Data written successfully and transaction committed.")
            
            except Exception as e:
                print(f"An error occurred: {e}")
                transaction.rollback()  # Rollback the transaction on error
                print("Transaction rolled back.")

    def close(self):
        if self.engine:
            self.engine.dispose()
            print("MySQL connection closed.")
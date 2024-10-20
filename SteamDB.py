"""
This file contains the SteamDB class. It will setup the data warehouse schema on an empty MySQL database.
"""

import pandas as pd
from sqlalchemy import create_engine, update, select, text, Table, MetaData
from mysql.connector import Error


class SteamDB:
    def __init__(self, db_config, csv_file="data/games_fixed.csv"):
        self.csv_file = csv_file
        self.db_config = db_config
        self.df = None
        self.engine = None
        self.metadata = None
        
        # Internal Setup
        self.load_csv()
        self.create_connection()

    def load_csv(self):
        # Load CSV into a DataFrame
        try:
            self.df = pd.read_csv(self.csv_file)

            # Change the column names to match the database columns
            new_column_names = [
                'gameID', 'name', 'releaseDate', 'estimatedOwners',
                'peakCCU', 'requiredAge', 'price', 'dlcCount',
                'aboutTheGame', 'supportedLanguages', 'fullAudioLanguages',
                'reviews', 'headerImageHREF', 'websiteURL', 'supportURL',
                'supportEmail', 'windowsSupport', 'macSupport', 'linuxSupport',
                'metacriticScore', 'NewMetacriticUrl', 'userScore',
                'positive', 'negative', 'scoreRank', 'achievementCount',
                'recommendations', 'NewNotes', 'averagePlaytimeForever',
                'averagePlaytimeTwoWeeks', 'medianPlaytimeForever',
                'medianPlaytimeTwoWeeks', 'developer', 'publisher',
                'categories', 'genres', 'tags', 'NewScreenshots',
                'NewMovies'
            ]
            rename_mapping = dict(zip(self.df.columns, new_column_names))
            self.df.rename(columns=rename_mapping, inplace=True)
            
            # Remove rows with NULL gameID
            self.df = self.df[ self.df['gameID'].notnull() ]

            # Change format from "MMMM DD, YYYY" to MM-DD-YYYY for a DATE type
            self.df['releaseDate'] = pd.to_datetime(self.df['releaseDate'], format='mixed', dayfirst=True).dt.strftime('%Y-%m-%d')

            print("CSV loaded into DataFrame.")
        except Error as e:
            print(f"Failed to load CSV: {e}")

    def create_connection(self):
        # Create a connection to the MySQL server
        connection_string = f"mysql+mysqlconnector://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}"
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()
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
                print("SteamDB builder executed successfully!\nBinary encodings added to table 'dim_os'")
        except FileNotFoundError:
            print(f"Error: The file 'queries/db_builder.txt' was not found.")
        except Error as e:
            print(f"Failed to execute SQL query: {e}")

    # Populate the DB using `games_fixed.csv`
    def populate_steam_db(self):
        curr_action = "dim_game"
        try:
            dim_game_attr=['gameID', 'name', 'releaseDate', 'requiredAge', 'aboutTheGame',
                           'websiteURL', 'supportURL', 'supportEmail', 'supportedLanguages', 'fullAudioLanguages', 'headerImageHREF',
                           'categories', 'tags', 'genres']
            dim_game_df = self.df[
                (self.df['name'].notnull()) & (self.df['name'] != '') # Check for non-null and non-empty name
            ]
            dim_game_df.loc[:, 'aboutTheGame'] = dim_game_df['aboutTheGame'].str.slice(0, 255) # Truncate About the game to 255 characters
            dim_game_df.loc[:, 'supportURL'] = dim_game_df['supportURL'].str.slice(0, 255) # Truncate to 255 characters
            dim_game_df = dim_game_df.drop_duplicates(subset=['gameID', 'name']) 
            self.insert_to_mysql(dim_game_df[dim_game_attr], "dim_game")
            
            curr_action = "dim_company" # 59,807 rows
            dim_company_attr = ['developer', 'publisher']
            dim_company_df = self.df[dim_company_attr]
            dim_company_df.loc[:, 'developer'] = dim_company_df['developer'].str.slice(0, 255) # Truncate to 255 characters
            dim_company_df = dim_company_df.drop_duplicates()
            dim_company_df['companyID'] = range(1, len(dim_company_df) + 1)
            self.insert_to_mysql(dim_company_df, "dim_company")
            
            curr_action = "fact_GameMetrics"
            fact_attr = ['gameID', 'price', 'peakCCU', 'achievementCount', 
                         'averagePlaytimeForever', 'medianPlaytimeForever', 
                         'estimatedOwners', 'dlcCount', 'metacriticScore', 'userScore',
                         'positive', 'negative', 'scoreRank', 'recommendations']
            fact_df = self.df[
                (self.df['name'].notnull()) & (self.df['name'] != '') # Check for non-null and non-empty name
            ]
            fact_df = fact_df[fact_attr]
            
            curr_action = "fact table companyID"
            fact_df['companyID'] = self.get_company_id(dim_company_df, fact_df['gameID'])
            
            curr_action = "fact table osID"
            fact_df['osID'] = self.df.apply(lambda row: self.get_os_string(row['windowsSupport'],
                                                                           row['macSupport'],
                                                                           row['linuxSupport']), axis=1)
            self.insert_to_mysql(fact_df, "fact_gamemetrics")
            
        except Error as e:
            print(f"Failed to populate '{curr_action}': {e}")
            
    def get_company_id(self, company_df: pd.DataFrame, game_ids: pd.Series):
        result = self.df[self.df['gameID'].isin(game_ids)][['developer', 'publisher']]
        merged_result = pd.merge(result, 
                                company_df, 
                                on=['developer', 'publisher'], 
                                how='inner')
        return pd.Series(merged_result['companyID'])
    
    def get_os_string(self, windows, mac, linux):
        windows_bit = 1 if windows else 0
        mac_bit = 1 if mac else 0
        linux_bit = 1 if linux else 0
        return f"{windows_bit}{mac_bit}{linux_bit}"

    def execute_sql(self, sql_query, with_results = False):
        with self.engine.connect() as connection:
            result = connection.execute(text(sql_query))
            res_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            print(res_df)
            return res_df if with_results else None

    def insert_to_mysql(self, df: pd.DataFrame, table_name):
        with self.engine.connect() as connection:
            # Begin a transaction
            transaction = connection.begin()
            try:
                # Write the DataFrame to a SQL table
                df.to_sql(table_name, 
                          con=connection, 
                          if_exists='append', 
                          index=False,
                          chunksize=1000)
                
                # Commit the transaction if successful
                transaction.commit()
                print(f"Data written to {table_name} successfully and transaction committed.")
            
            except Exception as e:
                print(f"An error occurred populating {table_name}: {e}")
                transaction.rollback()  # Rollback the transaction on error
                print("Transaction rolled back.")

    def close(self):
        if self.engine:
            self.engine.dispose()
            print("MySQL connection closed.")
import psycopg2
from psycopg2 import pool
import threading
import uuid
import time
import json
from typing import Dict, List

class DistributedConcurrencyManager:
    def __init__(self):
        # Connection pools for three database nodes
        self.connection_pools = {
            'central': psycopg2.pool.SimpleConnectionPool(1, 20, 
                host='localhost', port='5432', 
                database='steam_games_central',
                user='admin', 
                password='we<3stadvdb'),
            'update': psycopg2.pool.SimpleConnectionPool(1, 20, 
                host='localhost', port='5433', 
                database='steam_games_update',
                user='admin', 
                password='we<3stadvdb'),
            'replica': psycopg2.pool.SimpleConnectionPool(1, 20, 
                host='localhost', port='5434', 
                database='steam_games_replica',
                user='admin', 
                password='we<3stadvdb')
        }
        
        # Transaction tracking
        self.transaction_log = []
        
    def log_transaction(self, transaction_id: str, scenario: str, nodes: List[str], status: str):
        """
        Log transaction details
        """
        transaction_entry = {
            'transaction_id': transaction_id,
            'timestamp': time.time(),
            'scenario': scenario,
            'nodes': nodes,
            'status': status
        }
        self.transaction_log.append(transaction_entry)
        
    def export_transaction_log(self, filename: str = 'concurrency_test.log'):
        """
        Export transaction log to a file
        """
        try:
            with open(filename, 'w') as log_file:
                json.dump(self.transaction_log, log_file, indent=2)
            print(f"Transaction log exported to {filename}")
        except Exception as e:
            print(f"Error exporting transaction log: {e}")
            
    ### TEST CASES ###

    def get_isolation_level(self, scenario: str) -> int:
        """
        Select appropriate isolation level based on scenario
        """
        isolation_levels = {
            'read': psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            'mixed': psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            'write': psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
        }
        return isolation_levels.get(scenario, psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)

    def begin_distributed_transaction(self, scenario: str):
        """
        Begin a distributed transaction with appropriate isolation level
        """
        transaction_id = str(uuid.uuid4())
        connections = {}
        
        try:
            # Get connections for all nodes
            for node, pool in self.connection_pools.items():
                conn = pool.getconn()
                # Set isolation level dynamically
                conn.set_isolation_level(self.get_isolation_level(scenario))
                conn.autocommit = False
                connections[node] = conn
            
            return transaction_id, connections
        
        except Exception as e:
            print(f"Transaction initialization error: {e}")
            return None, None

    def execute_concurrent_read_scenario(self):
        """
        Case #1: Concurrent read transactions across nodes
        """
        transaction_id, connections = self.begin_distributed_transaction('read')
        
        def node_read(node, conn):
            try:
                cursor = conn.cursor()
                # Simulate reading game data
                cursor.execute("SELECT * FROM steam_games WHERE price > 10")
                results = cursor.fetchall()
                print(f"Read Transaction {transaction_id} on {node}: {len(results)} rows")
            except Exception as e:
                print(f"Read error on {node}: {e}")
        
        # Concurrent reads across nodes
        threads = [
            threading.Thread(target=node_read, args=(node, conn)) 
            for node, conn in connections.items()
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Commit transaction
        try:
            for conn in connections.values():
                conn.commit()
            # Log successful transaction
            self.log_transaction(
                transaction_id, 
                'concurrent_read', 
                list(connections.keys()), 
                'committed'
            )
        except Exception as e:
            # Log failed transaction
            self.log_transaction(
                transaction_id, 
                'concurrent_read', 
                list(connections.keys()), 
                'failed'
            )

    def execute_mixed_read_write_scenario(self):
        """
        Case #2: Mixed read and write transactions
        """
        transaction_id, connections = self.begin_distributed_transaction('mixed')
        
        try:
            # Write operation on central node
            central_conn = connections['central']
            central_cursor = central_conn.cursor()
            central_cursor.execute(
                "UPDATE steam_games SET price = price * 1.1 WHERE developer = 'Valve'"
            )
            
            # Simultaneous read operations on other nodes
            def node_read(node, conn):
                try:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM steam_games WHERE developer = 'Valve'")
                    results = cursor.fetchall()
                    print(f"Read Transaction {transaction_id} on {node}: {len(results)} rows")
                except Exception as e:
                    print(f"Read error on {node}: {e}")
            
            read_threads = [
                threading.Thread(target=node_read, args=(node, conn)) 
                for node, conn in connections.items() if node != 'central'
            ]
            
            for thread in read_threads:
                thread.start()
            
            for thread in read_threads:
                thread.join()
            
            # Commit transaction
            for conn in connections.values():
                conn.commit()
            
            # Log successful transaction
            self.log_transaction(
                transaction_id, 
                'mixed_read_write', 
                list(connections.keys()), 
                'committed'
            )
        
        except Exception as e:
            # Rollback in case of error
            for conn in connections.values():
                conn.rollback()
            print(f"Mixed read-write transaction failed: {e}")
            
            # Log failed transaction
            self.log_transaction(
                transaction_id, 
                'mixed_read_write', 
                list(connections.keys()), 
                'failed'
            )

    def execute_concurrent_write_scenario(self):
        """
        Case #3: Concurrent write transactions
        """
        transaction_id, connections = self.begin_distributed_transaction('write')
        
        def simultaneous_update(node, conn):
            try:
                cursor = conn.cursor()
                # Simulate updating game prices
                cursor.execute(
                    "UPDATE steam_games SET price = price * 1.05 WHERE price > 10"
                )
                print(f"Write Transaction {transaction_id} on {node} completed")
            except Exception as e:
                print(f"Write error on {node}: {e}")
        
        # Concurrent writes across nodes
        threads = [
            threading.Thread(target=simultaneous_update, args=(node, conn)) 
            for node, conn in connections.items()
        ]
        
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Commit transaction
        try:
            for conn in connections.values():
                conn.commit()
            
            # Log successful transaction
            self.log_transaction(
                transaction_id, 
                'concurrent_write', 
                list(connections.keys()), 
                'committed'
            )
        except Exception as e:
            # Log failed transaction
            self.log_transaction(
                transaction_id, 
                'concurrent_write', 
                list(connections.keys()), 
                'failed'
            )

    def run_concurrency_scenarios(self):
        """
        Execute all concurrency scenarios
        """
        print("Running Concurrent Read Scenario...")
        self.execute_concurrent_read_scenario()
        
        print("\nRunning Mixed Read-Write Scenario...")
        self.execute_mixed_read_write_scenario()
        
        print("\nRunning Concurrent Write Scenario...")
        self.execute_concurrent_write_scenario()

# Execute the concurrency scenarios
concurrency_manager = DistributedConcurrencyManager()
concurrency_manager.run_concurrency_scenarios()

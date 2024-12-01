from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
import psycopg2
import uuid
import threading
import time
import logging
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseNode:
    def __init__(self, node_id: str, is_central: bool = False, slave_nodes: List[str] = None):
        self.id = node_id
        self.is_central = is_central
        self.conn = self.connect_to_database(node_id)
        self.transactions = {}
        self.current_tx = 'None'
        
        # Track slave nodes for replication
        self.slave_nodes = slave_nodes or []
        
        # Replication settings
        self.replication_interval = 60  # Default: replicate every 60 seconds
        self.replication_thread = None
        self.stop_replication = threading.Event()
        
         # Crash and Recovery settings
        self.recovery_log = {}
        self.is_available = True
        self.recovery_interval = 30  # Attempt recovery every 30 seconds
        self.recovery_thread = None
        self.stop_recovery = threading.Event()
        self.last_crash_time = None

    def connect_to_database(self, node_id: str):
        if node_id == 'Node-1':
            return psycopg2.connect(
                host='localhost', port=5432, database='steam_games_central',
                user='admin', password='we<3stadvdb'
            )
        elif node_id == 'Node-2':
            return psycopg2.connect(
                host='localhost', port=5433, database='steam_games_update',
                user='admin', password='we<3stadvdb' 
            )
        elif node_id == 'Node-3':
            return psycopg2.connect(
                host='localhost', port=5434, database='steam_games_replica',
                user='admin', password='we<3stadvdb'
            )
        else:
            raise ValueError(f'Invalid node ID: {node_id}')

    def begin_transaction(self, isolation_level: str) -> str:
        if not self.is_available:
            self.current_tx = 'None'
            return
        
        tx_id = str(uuid.uuid4())
        self.transactions[tx_id] = {
            'isolation_level': isolation_level,
            'status': 'ACTIVE',
            'output': ''
        }
        self.recovery_log[tx_id] = {
            'isolation_level': isolation_level,
            'status': 'STARTED',
            'operation': None,
            'timestamp': time.time()
        }
        self.current_tx = tx_id
        return tx_id

    def execute_transaction(self, tx_id: str, query: str, params: tuple = None, max_retries: int = 3, retry_delay: float = 1.0) -> bool:
        if tx_id not in self.transactions or self.transactions[tx_id]['status'] != 'ACTIVE':
            raise ValueError('Invalid transaction')
        
        if not self.is_available:
            return

        tx = self.transactions[tx_id]
        retries = 0

        while retries <= max_retries:
            try:
                # Begin a new transaction with the desired isolation level
                self.conn.autocommit = False
                with self.conn.cursor() as cur:
                    cur.execute(f"BEGIN; SET TRANSACTION ISOLATION LEVEL {tx['isolation_level'].replace('_', ' ')};")
                    cur.execute(query)
                    tx['output'] = str(cur.fetchall())

                    self.conn.commit()
                    tx['status'] = 'COMMITTED'

                    if self.is_central and tx['status'] == 'COMMITTED':
                        replication_data = {
                            'query': query,
                            'params': params or ()
                        }
                        self.replicate_data(transaction_data=replication_data)
                        
                    # Store operation for potential recovery
                    self.recovery_log[tx_id]['operation'] = cur.query
                    self.recovery_log[tx_id]['status'] = 'COMMITTED'

                    return True

            except psycopg2.OperationalError as e:
                # Handle deadlocks or lock contention
                if isinstance(e, psycopg2.errors.DeadlockDetected):
                    self.conn.rollback()
                    retries += 1
                    if retries <= max_retries:
                        time.sleep(retry_delay)  # Wait before retrying
                        continue
                else:
                    self.conn.rollback()
                    tx['status'] = 'ROLLED_BACK'
                    tx['output'] = str(e)
                    self.recovery_log[tx_id]['status'] = 'FAILED'
                    raise e

            except Exception as e:
                self.conn.rollback()
                tx['status'] = 'ROLLED_BACK'
                tx['output'] = str(e)
                self.recovery_log[tx_id]['status'] = 'FAILED'
                raise e

        # If we exceed retries, raise an exception
        tx['status'] = 'ROLLED_BACK'
        tx['output'] = "Transaction failed after max retries due to lock contention."
        raise RuntimeError("Transaction failed after max retries due to lock contention.")
    
    ### REPLICATION MECHANISM ###
    
    def replicate_data(self, table_name: str = 'steam_games', transaction_data: Dict = None) -> Dict[str, Any]:
        """
        Replicate data from master node to slave nodes.
        
        Args:
            table_name (str): Name of the table to replicate. Defaults to 'steam_games'.
        
        Returns:
            Dict tracking replication status for each slave node.
        """
        # Only master node can initiate replication
        if not self.is_central:
            raise ValueError("Only master node can initiate replication")

        # Replication results
        replication_status = {}

        # Fetch data to be replicated
        with self.conn.cursor() as cur:
            if transaction_data:
                # Replicate specific transaction data
                cur.execute(transaction_data['query'], transaction_data.get('params', ()))
                master_data = cur.fetchall()
            else:
                # Full table replication
                cur.execute(f"SELECT * FROM {table_name}")
                master_data = cur.fetchall()

        # Replicate to each slave node
        for slave_node_id in self.slave_nodes:
            try:
                # Connect to slave node
                slave_conn = self.connect_to_database(slave_node_id)
                
                with slave_conn.cursor() as slave_cur:
                    # Start a transaction
                    slave_cur.execute("BEGIN;")
                    
                    if transaction_data:
                        # Replicate specific transaction
                        placeholders = ', '.join(['%s'] * len(transaction_data['params']))
                        replicate_query = transaction_data['query']
                        slave_cur.execute(replicate_query, transaction_data['params'])
                    else:
                        # Full table replication
                        # Delete existing data in the slave table
                        slave_cur.execute(f"DELETE FROM {table_name}")
                        
                        # Prepare and execute insert statements for each row
                        for row in master_data:
                            placeholders = ', '.join(['%s'] * len(row))
                            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                            slave_cur.execute(insert_query, row)
                    
                    # Commit the transaction
                    slave_conn.commit()
                    
                    replication_status[slave_node_id] = {
                        'status': 'SUCCESS',
                        'rows_replicated': len(master_data) if not transaction_data else 1
                    }
                
                # Close slave connection
                slave_conn.close()

            except Exception as e:
                replication_status[slave_node_id] = {
                    'status': 'FAILED',
                    'error': str(e)
                }

        return replication_status

    def start_periodic_replication(self, interval: int = 60):
        """
        Start a background thread for periodic data replication.
        
        Args:
            interval (int): Replication interval in seconds. Defaults to 60.
        """
        if not self.is_central:
            raise ValueError("Only master node can start periodic replication")

        self.replication_interval = interval
        self.stop_replication.clear()

        def replicate_periodically():
            while not self.stop_replication.is_set():
                try:
                    self.replicate_data()
                except Exception as e:
                    print(f"Replication error: {e}")
                
                # Sleep for the specified interval
                time.sleep(self.replication_interval)

        self.replication_thread = threading.Thread(target=replicate_periodically, daemon=True)
        self.replication_thread.start()

    def stop_periodic_replication(self):
        """
        Stop the periodic replication thread.
        """
        if self.replication_thread and self.replication_thread.is_alive():
            self.stop_replication.set()
            self.replication_thread.join()
        
    ### CRASH AND RECOVERY ###
    
    def start_automatic_recovery(self, interval: int = 5):
        """
        Start a background thread for automatic periodic recovery attempts
        
        Args:
            interval (int): Recovery check interval in seconds. Defaults to 30.
        """
        self.stop_periodic_replication()
        self.recovery_interval = interval
        self.stop_recovery.clear()

        def attempt_recovery_periodically():
            while not self.stop_recovery.is_set():
                if not self.is_available:
                    try:
                        self.recover()
                        # If recovery succeeds, stop the periodic recovery
                        if self.is_available:
                            break
                    except Exception as e:
                        logging.error(f"Automatic recovery attempt failed for {self.id}: {e}")
                
                # Sleep for the specified interval
                time.sleep(self.recovery_interval)

        self.recovery_thread = threading.Thread(target=attempt_recovery_periodically, daemon=True)
        self.recovery_thread.start()

    def stop_automatic_recovery(self):
        """
        Stop the automatic recovery thread
        """
        if self.recovery_thread and self.recovery_thread.is_alive():
            self.stop_recovery.set()
            self.recovery_thread.join()
            self.start_periodic_replication()

    def simulate_crash(self):
        """Simulate node crash by closing connection and marking as unavailable"""
        if self.conn:
            self.conn.close()
        self.conn = None
        self.is_available = False
        self.last_crash_time = time.time()
        logger.warning(f"Node {self.id} has crashed")
        
        # Start automatic recovery attempts
        self.start_automatic_recovery()

    def recover(self):
        """Attempt to recover node and reconnect to database"""
        try:
            # Attempt to reestablish database connection
            self.conn = self.connect_to_database(self.id)
            
            # Mark node as available
            self.is_available = True
            
            # Replay recovery log to ensure consistency
            self.replay_recovery_log()
            
            # Stop the automatic recovery thread
            self.stop_automatic_recovery()
            
            logger.info(f"Node {self.id} recovered successfully")
            
            return True
        except Exception as e:
            logger.error(f"Recovery failed for {self.id}: {e}")
            return False
        
    def replay_recovery_log(self):
        """Replay transactions from recovery log to ensure consistency"""
        if not self.recovery_log:
            return

        with self.conn.cursor() as cur:
            for tx_id, transaction in self.recovery_log.items():
                if transaction['status'] == 'COMMITTED':
                    try:
                        # Replay the exact transaction
                        cur.execute(transaction['operation'])
                        self.conn.commit()
                        logger.info(f"Replayed transaction {tx_id}")
                    except Exception as e:
                        logger.error(f"Failed to replay transaction {tx_id}: {e}")
                        self.conn.rollback()


app = Flask(__name__)
CORS(app)

# Initialize nodes
central_node = DatabaseNode('Node-1', is_central=True, slave_nodes=['Node-2', 'Node-3'])
update_node_2 = DatabaseNode('Node-2')
update_node_3 = DatabaseNode('Node-3')

@app.route('/')
def index():
    central_node.start_periodic_replication()
    return render_template('flask_frontend_with_crash.html')

@app.route('/case1', methods=['POST'])
def case1_concurrent_reads():
    # Case #1: Concurrent transactions in two or more nodes are reading the same data item.
    try:
        central_node.current_tx = 'None'
        tx_node_2 = update_node_2.begin_transaction('READ_COMMITTED')
        tx_node_3 = update_node_3.begin_transaction('READ_COMMITTED')

        def node_2_read():
            query = "SELECT title FROM steam_games WHERE developer = 'Valve';"
            update_node_2.execute_transaction(tx_node_2, query)

        def node_3_read():
            query = "SELECT title, price FROM steam_games WHERE price > 10;"
            update_node_3.execute_transaction(tx_node_3, query)
        
        # Run updates concurrently
        thread1 = threading.Thread(target=node_2_read)
        thread2 = threading.Thread(target=node_3_read)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/case2', methods=['POST'])
def case2_mix_read_write():
    # Case #2: At least one transaction in the three nodes is writing (update / delete) 
    # and the other concurrent transactions are reading the same data item.
    try:
        write_tx = central_node.begin_transaction('REPEATABLE_READ')
        read_tx = update_node_2.begin_transaction('READ_COMMITTED')
        update_node_3.current_tx = 'None'

        def write_in_central():
            query = "UPDATE steam_games SET price = price + 1 WHERE price < 10 RETURNING title, price;"
            central_node.execute_transaction(write_tx, query)

        def read_with_node_2():
            query = "SELECT title, publisher FROM steam_games WHERE price = 14.99;"
            update_node_2.execute_transaction(read_tx, query)
        
        # Run updates concurrently
        thread1 = threading.Thread(target=write_in_central)
        thread2 = threading.Thread(target=read_with_node_2)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/case3', methods=['POST'])
def case3_concurrent_writes():
    # Case #3: Concurrent transactions in two or more nodes are writing (update / delete) the same data item.
    try:
        # Define transactions and queries
        write_tx = central_node.begin_transaction('SERIALIZABLE')
        write_tx_2 = update_node_2.begin_transaction('SERIALIZABLE')
        update_node_3.current_tx = 'None'

        def update_data():
            query = "UPDATE steam_games SET price = price - 1 WHERE title = 'Counter-Strike' AND price > 4 RETURNING title, price;"
            return update_node_2.execute_transaction(write_tx, query)

        def update_same_data():
            query = "UPDATE steam_games SET price = price - 2 WHERE title = 'Counter-Strike' AND price > 4 RETURNING title, price;"
            return update_node_3.execute_transaction(write_tx_2, query)

        # Run updates concurrently
        thread1 = threading.Thread(target=update_data)
        thread2 = threading.Thread(target=update_same_data)

        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/node-info', methods=['GET'])
def get_node_info():
    return jsonify({
        'node1': {
            'tx_id': central_node.current_tx,
            'status': central_node.transactions.get(central_node.current_tx, {}).get('status', 'N/A'),
            'output': central_node.transactions.get(central_node.current_tx, {}).get('output', 'N/A')
        },
        'node2': {
            'tx_id': update_node_2.current_tx, 
            'status': update_node_2.transactions.get(update_node_2.current_tx, {}).get('status', 'N/A'),
            'output': update_node_2.transactions.get(update_node_2.current_tx, {}).get('output', 'N/A')
        },
        'node3': {
            'tx_id': update_node_3.current_tx,
            'status': update_node_3.transactions.get(update_node_3.current_tx, {}).get('status', 'N/A'),
            'output': update_node_3.transactions.get(update_node_3.current_tx, {}).get('output', 'N/A')
        }
    })
    
@app.route('/simulate-crash', methods=['POST'])
def simulate_crash():
    node_id = request.json.get('node_id')
    
    if node_id == 'Node-1':
        central_node.simulate_crash()
    elif node_id == 'Node-2':
        update_node_2.simulate_crash()
    elif node_id == 'Node-3':
        update_node_3.simulate_crash()
    
    return jsonify({
        'status': 'success', 
        'message': f'Simulated crash for {node_id}',
        'crash_time': time.time()
    })

@app.route('/node-recovery-status', methods=['GET'])
def get_node_recovery_status():
    return jsonify({
        'Node-1': {
            'is_available': central_node.is_available,
            'last_crash_time': central_node.last_crash_time
        },
        'Node-2': {
            'is_available': update_node_2.is_available,
            'last_crash_time': update_node_2.last_crash_time
        },
        'Node-3': {
            'is_available': update_node_3.is_available,
            'last_crash_time': update_node_3.last_crash_time
        }
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
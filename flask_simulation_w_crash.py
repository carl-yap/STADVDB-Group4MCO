from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from time import sleep
import psycopg2
import uuid
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseNode:
    def __init__(self, node_id: str, is_central: bool = False):
        self.id = node_id
        self.is_central = is_central
        self.conn = self.connect_to_database(node_id)
        self.transactions = {}
        self.current_tx = 'None'
        self.recovery_log = {}
        self.is_available = True

    def connect_to_database(self, node_id: str):
        try:
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
        except Exception as e:
            logger.error(f"Error connecting to {node_id}: {e}")
            self.conn = None
            self.is_available = False
        
    def simulate_crash(self):
        """Simulate node crash by closing connection and marking as unavailable"""
        if self.conn:
            self.conn.close()
        self.conn = None
        self.is_available = False
        logger.warning(f"Node {self.id} has crashed")
        
    def recover(self):
        """Attempt to recover node and reconnect to database"""
        try:
            self.connect_to_database(self.id)
            self.is_available = True
            self.replay_recovery_log()
            logger.info(f"Node {self.id} recovered successfully")
        except Exception as e:
            logger.error(f"Recovery failed for {self.id}: {e}")
            
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

    def begin_transaction(self, isolation_level: str) -> str:
        if not self.is_available:
            raise RuntimeError(f"Node {self.id} is currently unavailable")
        
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

    def execute_transaction(self, tx_id: str, operation: callable) -> bool:
        if tx_id not in self.transactions or self.transactions[tx_id]['status'] != 'ACTIVE':
            raise ValueError('Invalid transaction')
        
        if not self.is_available:
            raise RuntimeError(f"Node {self.id} is currently unavailable")

        tx = self.transactions[tx_id]
        try:
            with self.conn.cursor() as cur:
                if tx['isolation_level'] == 'REPEATABLE_READ':
                    cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
                    cur.execute("SELECT * FROM steam_games;")
                    tx['output'] = str(cur.fetchall())
                    operation(tx['output'])
                elif tx['isolation_level'] == 'SERIALIZABLE':
                    cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
                    cur.execute("SELECT * FROM steam_games;")
                    serializable_data = cur.fetchall()
                    tx['output'] = str(serializable_data)
                    operation(tx['output'])
                else:  # READ_COMMITTED
                    print("This should run for Case 1...")
                    # cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tx['output'] = str(cur.fetchall())
                    print(tx['output'])
                    operation(tx['output'])
                
                # Store operation for potential recovery
                self.recovery_log[tx_id]['operation'] = cur.query
                self.recovery_log[tx_id]['status'] = 'COMMITTED'

                self.conn.commit()
                tx['status'] = 'COMMITTED'
                return True
        except Exception as e:
            self.conn.rollback()
            tx['status'] = 'ROLLED_BACK'
            tx['output'] = str(e)
            self.recovery_log[tx_id]['status'] = 'FAILED'
            raise e

app = Flask(__name__)
CORS(app)

# Initialize nodes
central_node = DatabaseNode('Node-1', is_central=True)
update_node_2 = DatabaseNode('Node-2')
update_node_3 = DatabaseNode('Node-3')

@app.route('/')
def index():
    return render_template('flask_frontend.html')

@app.route('/case1', methods=['POST'])
def case1_concurrent_reads():
    print("Starting Case 1...")
    try:
        tx_node_2 = update_node_2.begin_transaction('READ_COMMITTED')
        tx_node_3 = update_node_3.begin_transaction('READ_COMMITTED')

        def read_from_node_2(data):
            update_node_2.transactions[tx_node_2]['output'] = data

        def read_from_node_3(data):
            update_node_3.transactions[tx_node_3]['output'] = data

        print("Executing txs for Case 1...")
        update_node_2.execute_transaction(tx_node_2, read_from_node_2)
        update_node_3.execute_transaction(tx_node_3, read_from_node_3)

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/case2', methods=['POST'])
def case2_mix_read_write():
    try:
        tx_node_1 = central_node.begin_transaction('READ_COMMITTED')
        tx_node_2 = update_node_2.begin_transaction('REPEATABLE_READ')

        def read_from_node_1(data):
            central_node.transactions[tx_node_1]['output'] = data

        def update_on_node_2(data):
            for game in eval(data):
                if game[1] == 'Portal':
                    game[4] += 1.00
                    break
            update_node_2.transactions[tx_node_2]['output'] = str(data)

        central_node.execute_transaction(tx_node_1, read_from_node_1)
        update_node_2.execute_transaction(tx_node_2, update_on_node_2)

        return jsonify({'status': 'success'}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/case3', methods=['POST'])
def case3_concurrent_writes():
    try:
        tx_node_2 = update_node_2.begin_transaction('SERIALIZABLE')
        tx_node_3 = update_node_3.begin_transaction('SERIALIZABLE')

        def update_on_node_2(data):
            for game in eval(data):
                if game[1] == 'Half-Life':
                    game[4] += 2.00
                    break
            update_node_2.transactions[tx_node_2]['output'] = str(data)

        def update_on_node_3(data):
            for game in eval(data):
                if game[1] == 'Counter-Strike':
                    game[4] += 3.00
                    break
            update_node_3.transactions[tx_node_3]['output'] = str(data)

        update_node_2.execute_transaction(tx_node_2, update_on_node_2)
        update_node_3.execute_transaction(tx_node_3, update_on_node_3)

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
    
    return jsonify({'status': 'success', 'message': f'Simulated crash for {node_id}'})

@app.route('/recover', methods=['POST'])
def recover_node():
    node_id = request.json.get('node_id')
    
    if node_id == 'Node-1':
        central_node.recover()
    elif node_id == 'Node-2':
        update_node_2.recover()
    elif node_id == 'Node-3':
        update_node_3.recover()
    
    return jsonify({'status': 'success', 'message': f'Recovered {node_id}'})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
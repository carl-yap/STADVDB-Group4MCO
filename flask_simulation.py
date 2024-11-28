from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from time import sleep
import psycopg2
import uuid

class DatabaseNode:
    def __init__(self, node_id: str, is_central: bool = False):
        self.id = node_id
        self.is_central = is_central
        self.conn = self.connect_to_database(node_id)
        self.transactions = {}
        self.current_tx = 'None'

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
        tx_id = str(uuid.uuid4())
        self.transactions[tx_id] = {
            'isolation_level': isolation_level,
            'status': 'ACTIVE',
            'output': ''
        }
        self.current_tx = tx_id
        return tx_id

    def execute_transaction(self, tx_id: str, operation: callable) -> bool:
        if tx_id not in self.transactions or self.transactions[tx_id]['status'] != 'ACTIVE':
            raise ValueError('Invalid transaction')

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

                self.conn.commit()
                tx['status'] = 'COMMITTED'
                return True
        except Exception as e:
            self.conn.rollback()
            tx['status'] = 'ROLLED_BACK'
            tx['output'] = str(e)
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

if __name__ == '__main__':
    app.run(debug=True, port=5000)
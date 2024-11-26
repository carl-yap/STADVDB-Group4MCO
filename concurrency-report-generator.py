import json
import csv
from datetime import datetime
import psycopg2
import statistics

class ConcurrencyReportGenerator:
    def __init__(self, log_file='concurrency_test.log', db_connections=None):
        """
        Initialize report generator with test logs and database connections
        
        Args:
            log_file (str): Path to concurrency test log file
            db_connections (dict): Database connection parameters
        """
        self.log_file = log_file
        self.db_connections = db_connections or {
            'central': {
                'host': 'localhost', 
                'port': '5432',
                'database': 'steam_games_central',
                'user': 'admin', 
                'password': 'we<3stadvdb'
            },
            'update': {
                'host': 'localhost', 
                'port': '5433',
                'database': 'steam_games_update',
                'user': 'admin', 
                'password': 'we<3stadvdb'
            },
            'replica': {
                'host': 'localhost', 
                'port': '5434',
                'database': 'steam_games_replica',
                'user': 'admin', 
                'password': 'we<3stadvdb'
            }
        }
        
        # Metrics containers
        self.concurrency_metrics = {
            'read_scenario': {},
            'mixed_scenario': {},
            'write_scenario': {}
        }

    def parse_log_file(self):
        """
        Parse concurrency test log file and extract key metrics
        """
        try:
            with open(self.log_file, 'r') as log:
                lines = log.readlines()
                
            # Parse log lines and extract metrics
            for line in lines:
                # Example parsing logic (adjust based on actual log format)
                if 'Read Transaction' in line:
                    self._parse_read_metrics(line)
                elif 'Write Transaction' in line:
                    self._parse_write_metrics(line)
        
        except FileNotFoundError:
            print(f"Log file {self.log_file} not found.")

    def _parse_read_metrics(self, log_line):
        """
        Extract metrics from read transaction log lines
        """
        # Extract transaction ID, node, and number of rows
        # Implement parsing logic based on your log format
        pass

    def _parse_write_metrics(self, log_line):
        """
        Extract metrics from write transaction log lines
        """
        # Similar parsing logic for write transactions
        pass

    def analyze_database_consistency(self):
        """
        Analyze data consistency across nodes after concurrent transactions
        """
        consistency_report = {}
        
        try:
            for node_name, conn_params in self.db_connections.items():
                conn = psycopg2.connect(**conn_params)
                cursor = conn.cursor()
                
                # Compare key metrics across nodes
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        AVG(price) as avg_price,
                        MAX(last_updated) as latest_update
                    FROM steam_games
                """)
                
                result = cursor.fetchone()
                consistency_report[node_name] = {
                    'total_records': result[0],
                    'average_price': result[1],
                    'latest_update': result[2]
                }
                
                conn.close()
        
        except psycopg2.Error as e:
            print(f"Database consistency check error: {e}")
        
        return consistency_report

    def generate_detailed_report(self):
        """
        Generate comprehensive concurrency test report
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'concurrency_scenarios': {
                'read_scenario': {
                    'total_transactions': 0,
                    'successful_transactions': 0,
                    'avg_response_time': 0.0,
                    'conflicts_detected': 0
                },
                'mixed_scenario': {
                    'total_transactions': 0,
                    'successful_transactions': 0,
                    'avg_response_time': 0.0,
                    'conflicts_detected': 0
                },
                'write_scenario': {
                    'total_transactions': 0,
                    'successful_transactions': 0,
                    'avg_response_time': 0.0,
                    'conflicts_detected': 0
                }
            },
            'database_consistency': self.analyze_database_consistency(),
            'isolation_level_effectiveness': {
                'read_scenario': 'Read Committed',
                'mixed_scenario': 'Repeatable Read',
                'write_scenario': 'Serializable'
            }
        }
        
        return report

    def export_report(self, report, format='json'):
        """
        Export report in specified format
        
        Args:
            report (dict): Concurrency test report
            format (str): Export format (json/csv)
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if format == 'json':
            filename = f'concurrency_report_{timestamp}.json'
            with open(filename, 'w') as f:
                json.dump(report, f, indent=4)
            print(f"Report exported to {filename}")
        
        elif format == 'csv':
            filename = f'concurrency_report_{timestamp}.csv'
            with open(filename, 'w', newline='') as f:
                writer = csv.writer(f)
                # Write headers and data
                writer.writerow(['Scenario', 'Total Transactions', 'Successful Transactions', 'Avg Response Time'])
                for scenario, data in report['concurrency_scenarios'].items():
                    writer.writerow([
                        scenario, 
                        data['total_transactions'], 
                        data['successful_transactions'], 
                        data['avg_response_time']
                    ])
            print(f"Report exported to {filename}")

    def run(self):
        """
        Execute complete report generation process
        """
        # Parse log file
        self.parse_log_file()
        
        # Generate detailed report
        detailed_report = self.generate_detailed_report()
        
        # Export in multiple formats
        self.export_report(detailed_report, 'json')
        self.export_report(detailed_report, 'csv')
        
        return detailed_report

# Execute report generation
if __name__ == "__main__":
    report_generator = ConcurrencyReportGenerator()
    final_report = report_generator.run()
    
    # Optional: Print key insights
    print("\n--- Concurrency Test Report Insights ---")
    for scenario, metrics in final_report['concurrency_scenarios'].items():
        print(f"\n{scenario.replace('_', ' ').title()}:")
        print(f"  Total Transactions: {metrics['total_transactions']}")
        print(f"  Successful Transactions: {metrics['successful_transactions']}")
        print(f"  Average Response Time: {metrics['avg_response_time']:.4f} seconds")

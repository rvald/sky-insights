import os
import sqlite3


class HybridMessageLogger:
    # Initialize database file paths
    TRANSIENT_DB_PATH = 'state/transient.db'
    FAILED_DB_PATH = 'state/failed.db'

    def __init__(self):
        self.transient_conn = None
        self.failed_conn = None
    
    def initialize(self):
        """Initialize the databases and ensure the directories exist."""
        self.ensure_directories()

        # Connect to SQLite databases
        self.transient_conn = sqlite3.connect(self.TRANSIENT_DB_PATH)
        self.failed_conn = sqlite3.connect(self.FAILED_DB_PATH)

        # Create tables if they don't exist
        self.create_table(self.transient_conn)
        self.create_table(self.failed_conn)

    def ensure_directories(self):
        """Ensure that the necessary directories and database files exist."""
        transient_directory = os.path.dirname(self.TRANSIENT_DB_PATH)
        failed_directory = os.path.dirname(self.FAILED_DB_PATH)

        os.makedirs(transient_directory, exist_ok=True)
        os.makedirs(failed_directory, exist_ok=True)

    def create_table(self, conn):
        """Create a table for storing events if it doesn't exist."""
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS events (
                key TEXT PRIMARY KEY,
                data BLOB
            )
        ''')
        conn.commit()

    def add_event(self, event_key, event_data):
        """Add or update an event in the transient database."""
        try:
            cursor = self.transient_conn.cursor()
            cursor.execute('''
                INSERT INTO events (key, data) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET data=excluded.data
            ''', (event_key, event_data))
            self.transient_conn.commit()
        except sqlite3.Error as e:
            print(f"Error adding event: {e}")
            raise Exception(f"Error adding event: {e}")

    def remove_event(self, event_key):
        """Remove an event from the transient database."""
        try:
            cursor = self.transient_conn.cursor()
            cursor.execute('DELETE FROM events WHERE key = ?', (event_key,))
            self.transient_conn.commit()
        except sqlite3.Error as e:
            print(f"Error removing event: {e}")
            raise Exception(f"Error removing event: {e}")

    def move_to_failed(self, event_key):
        """Move an event from the transient database to the failed database."""
        try:
            cursor = self.transient_conn.cursor()
            cursor.execute('SELECT data FROM events WHERE key = ?', (event_key,))
            row = cursor.fetchone()
            if row is not None:
                event_data = row[0]
                # Add to failed database
                cursor = self.failed_conn.cursor()
                cursor.execute('''
                    INSERT INTO events (key, data) VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET data=excluded.data
                ''', (event_key, event_data))
                self.failed_conn.commit()

                # Remove from transient database
                self.remove_event(event_key)
        except sqlite3.Error as e:
            print(f"Error moving event to failed: {e}")

    def close(self):
        """Close database connections."""
        if self.transient_conn:
            self.transient_conn.close()
            self.transient_conn = None
        if self.failed_conn:
            self.failed_conn.close()
            self.failed_conn = None
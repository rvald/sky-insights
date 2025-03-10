import os
import sqlite3
import aiosqlite

class HybridMessageLogger:
    TRANSIENT_DB_PATH = 'state/transient.db'
    FAILED_DB_PATH = 'state/failed.db'

    def __init__(self):
        self.transient_conn = None
        self.failed_conn = None

    async def initialize(self):
        """
        Initialize the databases (asynchronously) and ensure the directories exist.
        """
        self.ensure_directories()

        await self.close()

        # Connect to SQLite databases asynchronously
        self.transient_conn = await aiosqlite.connect(self.TRANSIENT_DB_PATH)
        self.failed_conn = await aiosqlite.connect(self.FAILED_DB_PATH)

        # Create tables if they don't exist
        await self.create_table(self.transient_conn)
        await self.create_table(self.failed_conn)

    def ensure_directories(self):
        """
        Ensure that the necessary directories and database files exist.
        (Still synchronous, but usually not a performance bottleneck.)
        """
        transient_directory = os.path.dirname(self.TRANSIENT_DB_PATH)
        failed_directory = os.path.dirname(self.FAILED_DB_PATH)

        os.makedirs(transient_directory, exist_ok=True)
        os.makedirs(failed_directory, exist_ok=True)

    async def create_table(self, conn: aiosqlite.Connection):
        """
        Create a table for storing events if it doesn't exist, using async DB calls.
        """
        async with conn.cursor() as cursor:
            await cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    key TEXT PRIMARY KEY,
                    data BLOB
                )
            ''')
        await conn.commit()

    async def add_event(self, event_key: str, event_data: bytes):
        """
        Add or update an event in the transient database (asynchronously).
        """
        try:
            async with self.transient_conn.cursor() as cursor:
                await cursor.execute('''
                    INSERT INTO events (key, data)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET data=excluded.data
                ''', (event_key, event_data))
            await self.transient_conn.commit()

            # Debug: fetch count
            async with self.transient_conn.cursor() as cursor:
                await cursor.execute('SELECT COUNT(*) FROM events')
                count = await cursor.fetchone()
                print(f"Transient DB event count: {count[0]}")
        except sqlite3.Error as e:
            print(f"Error adding event: {e}")
            raise Exception(f"Error adding event: {e}")

    async def remove_event(self, event_key: str):
        """
        Remove an event from the transient database (asynchronously).
        """
        try:
            async with self.transient_conn.cursor() as cursor:
                await cursor.execute('DELETE FROM events WHERE key = ?', (event_key,))
            await self.transient_conn.commit()
        except sqlite3.Error as e:
            print(f"Error removing event: {e}")
            raise Exception(f"Error removing event: {e}")

    async def move_to_failed(self, event_key: str):
        """
        Move an event from the transient database to the failed database (asynchronously).
        """
        try:
            # Fetch the event data from transient
            async with self.transient_conn.cursor() as t_cursor:
                await t_cursor.execute('SELECT data FROM events WHERE key = ?', (event_key,))
                row = await t_cursor.fetchone()

            if row is not None:
                event_data = row[0]

                # Add to failed database
                async with self.failed_conn.cursor() as f_cursor:
                    await f_cursor.execute('''
                        INSERT INTO events (key, data) VALUES (?, ?)
                        ON CONFLICT(key) DO UPDATE SET data=excluded.data
                    ''', (event_key, event_data))
                await self.failed_conn.commit()

                # Remove from transient database
                await self.remove_event(event_key)

        except sqlite3.Error as e:
            print(f"Error moving event to failed: {e}")

    async def close(self):
        """
        Close database connections (asynchronously).
        """
        if self.transient_conn:
            await self.transient_conn.close()
            self.transient_conn = None

        if self.failed_conn:
            await self.failed_conn.close()
            self.failed_conn = None
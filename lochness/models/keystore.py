"""
KeyStore class for managing API keys and secrets.
"""

from __future__ import annotations
from typing import Dict, List
from pydantic import BaseModel

from lochness.helpers import db


class KeyStore(BaseModel):
    """
    KeyStore class for managing API keys and secrets.
    """

    key_name: str
    key_value: str
    key_type: str
    key_metadata: Dict[str, str] = {}

    @staticmethod
    def init_db_table_query() -> List[str]:
        """
        Returns the SQL query to create the database table for keys.
        """
        enable_extension_query = """
            CREATE EXTENSION IF NOT EXISTS pgcrypto;
        """
        sql_query = """
            CREATE TABLE IF NOT EXISTS key_store (
                key_name TEXT NOT NULL,
                key_value BYTEA NOT NULL,
                key_type TEXT NOT NULL,
                key_metadata JSONB,
                PRIMARY KEY (key_name)
            );
        """
        return [enable_extension_query, sql_query]

    @staticmethod
    def drop_db_table_query() -> str:
        """
        Returns the SQL query to drop the database table for keys.
        """
        sql_query = """
            DROP TABLE IF EXISTS key_store;
        """
        return sql_query

    def to_sql_query(self, encryption_passphrase: str) -> str:
        """
        Converts the KeyStore instance to a SQL insert statement.
        """
        key_name = db.sanitize_string(self.key_name)
        key_value = db.sanitize_string(self.key_value)
        key_type = db.sanitize_string(self.key_type)
        key_metadata = db.sanitize_json(self.key_metadata)

        sql = f"""
            INSERT INTO key_store (
                key_name,
                key_value,
                key_type,
                key_metadata
            ) VALUES (
                '{key_name}',
                pgp_sym_encrypt('{key_value}', '{encryption_passphrase}'),
                '{key_type}',
                '{key_metadata}'
            ) ON CONFLICT (key_name)
            DO UPDATE SET key_value = EXCLUDED.key_value,
                          key_type = EXCLUDED.key_type,
                          key_metadata = EXCLUDED.key_metadata;
        """
        return sql

    @staticmethod
    def retrieve_key_query(key_name: str, encryption_passphrase: str) -> str:
        """
        Returns the SQL query to retrieve a key from the database.
        """
        key_name = db.sanitize_string(key_name)
        sql = f"""
            SELECT pgp_sym_decrypt(key_value, '{encryption_passphrase}') AS key_value
            FROM key_store
            WHERE key_name = '{key_name}';
        """
        return sql

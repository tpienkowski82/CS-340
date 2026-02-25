"""
animal_shelter.py

CRUD module for MongoDB (aac database, animals collection)

Author: Tom Pienkowski
Course: CS-340 / Project One
"""

from __future__ import annotations

from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

class AnimalShelter:
    """
    MongoDB CRUD wrapper for the AAC animal shelter dataset.

    Methods return simple, predictable values for easy reuse in scripts/notebooks:
      - create: bool
      - read: list
      - update: int (modified count)
      - delete: int (deleted count)
    """

    def __init__(
        self,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 27017,
        db_name: str = "aac",
        collection_name: str = "animals",
    ) -> None:
        """
        Initialize an authenticated MongoDB client and bind the target collection.

        Raises:
            ValueError: if username/password are empty
            ConnectionError: if connection/authentication fails
        """
        if not username or not password:
            raise ValueError("Username and password are required.")

        uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={db_name}"
        self._client: MongoClient = MongoClient(uri)

        self._database: Database = self._client[db_name]
        self._collection: Collection = self._database[collection_name]

        try:
            self._client.admin.command("ping")
        except PyMongoError as exc:
            raise ConnectionError("MongoDB connection/authentication failed.") from exc

    def create(self, data: Dict[str, Any]) -> bool:
        """
        Insert a single document.

        Args:
            data: Document to insert.

        Returns:
            True if the insert is acknowledged; otherwise False.
        """
        if not isinstance(data, dict) or not data:
            return False

        try:
            result = self._collection.insert_one(data)
            return bool(result.acknowledged)
        except PyMongoError:
            return False

    def read(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Query documents using find().

        Args:
            query: MongoDB filter document.

        Returns:
            A list of matching documents; empty list if none found or on error.
        """
        if not isinstance(query, dict):
            return []

        try:
            return list(self._collection.find(query))
        except PyMongoError:
            return []

    def update(self, query: Dict[str, Any], update_data: Dict[str, Any]) -> int:
        """
        Update documents that match the query.

        - If update_data includes an update operator (e.g., "$set"), it is used as provided.
        - Otherwise, update_data is wrapped in {"$set": update_data}.

        Args:
            query: MongoDB filter document.
            update_data: Update document or fields to set.

        Returns:
            Number of modified documents.
        """
        if not isinstance(query, dict) or not isinstance(update_data, dict) or not update_data:
            return 0

        try:
            update_doc = update_data if any(k.startswith("$") for k in update_data) else {"$set": update_data}
            result = self._collection.update_many(query, update_doc)
            return int(result.modified_count)
        except PyMongoError:
            return 0

    def delete(self, query: Dict[str, Any]) -> int:
        """
        Delete documents that match the query.

        Args:
            query: MongoDB filter document.

        Returns:
            Number of deleted documents.
        """
        if not isinstance(query, dict):
            return 0

        try:
            result = self._collection.delete_many(query)
            return int(result.deleted_count)
        except PyMongoError:
            return 0

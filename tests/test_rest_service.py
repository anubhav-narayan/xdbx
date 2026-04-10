"""Tests for XDBX REST service."""

import pytest
from fastapi.testclient import TestClient
from xdbx.service.rest_service import app, store

@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)

@pytest.fixture
def cleanup():
    """Clean up store after each test."""
    yield
    while len(store) > 0:
        key = next(iter(store))
        db = store.pop(key)
        db.close()

class TestRESTService:
    """Tests for the health check endpoint."""
    
    def test_health_check_basic(self, client, cleanup):
        """Test basic health check response."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "Success"
        assert data["message"] == "XDBX REST Service is running"
        assert data["service_version"] == "0.1.0"
        assert "databases_open" in data
        assert "system_metrics" in data
        assert "uptime" in data
    
    def test_health_check_has_system_metrics(self, client, cleanup):
        """Test that health check includes system metrics."""
        response = client.get("/")
        data = response.json()
        assert "system_metrics" in data
        system = data["system_metrics"]
        
        # Check process metrics
        assert "process" in system
        assert "memory_mb" in system["process"]
        assert "memory_percent" in system["process"]
        assert "cpu_percent" in system["process"]
        
        # Check system metrics
        assert "system" in system
        assert "memory_available_mb" in system["system"]
        assert "memory_percent" in system["system"]
        assert "disk_usage_percent" in system["system"]
    
    def test_health_check_uptime_format(self, client, cleanup):
        """Test that uptime is properly formatted."""
        response = client.get("/")
        data = response.json()
        assert "started_at" in data
        assert "uptime" in data
        assert "uptime_seconds" in data
        assert "current_time" in data
        assert isinstance(data["uptime_seconds"], int)


    """Tests for database CRUD operations."""
    
    def test_create_database(self, client, cleanup):
        """Test creating a database."""
        payload = {
            "name": "test_db",
            "autocommit": True,
            "journal_mode": "WAL",
            "flag": "c",
            "memory": False
        }
        response = client.post("/databases", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["status"] == "Success"
        assert data["database"] == "test_db"
    
    def test_create_database_memory(self, client, cleanup):
        """Test creating an in-memory database."""
        payload = {
            "name": "memory_db",
            "memory": True
        }
        response = client.post("/databases", json=payload)
        assert response.status_code == 201
        assert response.json()["status"] == "Success"
    
    def test_create_duplicate_database(self, client, cleanup):
        """Test that creating duplicate database returns error."""
        payload = {"name": "dup_db", "memory": True}
        client.post("/databases", json=payload)
        response = client.post("/databases", json=payload)
        assert response.status_code == 409
    
    def test_list_databases(self, client, cleanup):
        """Test listing databases."""
        client.post("/databases", json={"name": "db1", "memory": True})
        client.post("/databases", json={"name": "db2", "memory": True})
        
        response = client.get("/databases")
        assert response.status_code == 200
        data = response.json()
        assert "db1" in data["databases"]
        assert "db2" in data["databases"]
    
    def test_list_databases_empty(self, client, cleanup):
        """Test listing when no databases exist."""
        response = client.get("/databases")
        assert response.status_code == 200
        assert response.json()["databases"] == []
    
    def test_get_database_metadata(self, client, cleanup):
        """Test getting database metadata."""
        client.post("/databases", json={"name": "test_db", "memory": True})
        response = client.get("/databases/test_db")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "test_db"
        assert "filename" in data
        assert "autocommit" in data
    
    def test_get_nonexistent_database(self, client, cleanup):
        """Test getting metadata for nonexistent database."""
        response = client.get("/databases/nonexistent")
        assert response.status_code == 404
    
    def test_close_database(self, client, cleanup):
        """Test closing a database."""
        client.post("/databases", json={"name": "test_db", "memory": True})
        response = client.post("/databases/test_db/close")
        assert response.status_code == 200
        assert response.json()["status"] == "Success"
    
    def test_delete_database(self, client, cleanup):
        """Test deleting a database."""
        client.post("/databases", json={"name": "test_db", "memory": True})
        response = client.delete("/databases/test_db")
        assert response.status_code == 200
        assert response.json()["status"] == "Success"


    """Tests for storage operations."""
    
    @pytest.fixture
    def setup_db(self, client, cleanup):
        """Create a test database."""
        client.post("/databases", json={"name": "test_db", "memory": True})
    
    def test_create_json_storage(self, client, setup_db):
        """Test creating a JSON storage."""
        payload = {"name": "json_store", "storage_type": "json"}
        response = client.post("/databases/test_db/storages", json=payload)
        assert response.status_code == 201
        data = response.json()
        assert data["storage"] == "json_store"
        assert data["storage_type"] == "json"
    
    def test_create_table_storage(self, client, setup_db):
        """Test creating a table storage."""
        payload = {"name": "table_store", "storage_type": "table"}
        response = client.post("/databases/test_db/storages", json=payload)
        assert response.status_code == 201
        assert response.json()["storage_type"] == "table"
    
    def test_create_duplicate_storage(self, client, setup_db):
        """Test that creating duplicate storage returns error."""
        payload = {"name": "dup_store"}
        client.post("/databases/test_db/storages", json=payload)
        response = client.post("/databases/test_db/storages", json=payload)
        assert response.status_code == 409
    
    def test_list_storages(self, client, setup_db):
        """Test listing storages."""
        client.post("/databases/test_db/storages", json={"name": "store1"})
        client.post("/databases/test_db/storages", json={"name": "store2"})
        
        response = client.get("/databases/test_db/storages")
        assert response.status_code == 200
        data = response.json()
        names = [s["name"] for s in data["storages"]]
        assert "store1" in names
        assert "store2" in names
    
    def test_get_storage_metadata(self, client, setup_db):
        """Test getting storage metadata."""
        client.post("/databases/test_db/storages", json={"name": "test_store"})
        response = client.get("/databases/test_db/storages/test_store")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "test_store"
        assert "storage_type" in data
        assert "entries" in data
    
    def test_delete_storage(self, client, setup_db):
        """Test deleting a storage."""
        client.post("/databases/test_db/storages", json={"name": "test_store"})
        response = client.delete("/databases/test_db/storages/test_store")
        assert response.status_code == 200
        assert response.json()["status"] == "Success"


    """Tests for item CRUD operations."""
    
    @pytest.fixture
    def setup_storage(self, client, cleanup):
        """Create a test database with storage."""
        client.post("/databases", json={"name": "test_db", "memory": True})
        client.post("/databases/test_db/storages", json={"name": "test_store"})
    
    def test_put_item(self, client, setup_storage):
        """Test creating/updating an item."""
        payload = {"value": {"name": "test", "age": 25}}
        response = client.put(
            "/databases/test_db/storages/test_store/items/item1",
            json=payload
        )
        assert response.status_code == 200
        assert response.json()["status"] == "Success"
    
    def test_get_item(self, client, setup_storage):
        """Test getting an item."""
        client.put(
            "/databases/test_db/storages/test_store/items/item1",
            json={"value": {"name": "test"}}
        )
        response = client.get("/databases/test_db/storages/test_store/items/item1")
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "item1"
        assert data["value"]["name"] == "test"
    
    def test_get_nonexistent_item(self, client, setup_storage):
        """Test getting nonexistent item."""
        response = client.get("/databases/test_db/storages/test_store/items/nonexistent")
        assert response.status_code == 404
    
    def test_delete_item(self, client, setup_storage):
        """Test deleting an item."""
        client.put(
            "/databases/test_db/storages/test_store/items/item1",
            json={"value": {"test": "data"}}
        )
        response = client.delete("/databases/test_db/storages/test_store/items/item1")
        assert response.status_code == 200
        assert response.json()["status"] == "Success"
    
    def test_list_items(self, client, setup_storage):
        """Test listing items."""
        client.put(
            "/databases/test_db/storages/test_store/items/item1",
            json={"value": {"a": 1}}
        )
        client.put(
            "/databases/test_db/storages/test_store/items/item2",
            json={"value": {"b": 2}}
        )
        response = client.get("/databases/test_db/storages/test_store/items")
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2
    
    def test_bulk_upsert_items(self, client, setup_storage):
        """Test bulk upserting items."""
        payload = {
            "items": {
                "item1": {"a": 1},
                "item2": {"b": 2},
                "item3": {"c": 3}
            }
        }
        response = client.post(
            "/databases/test_db/storages/test_store/items",
            json=payload
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 3
    
    def test_list_items_with_pagination(self, client, setup_storage):
        """Test listing items with limit and offset."""
        for i in range(5):
            client.put(
                f"/databases/test_db/storages/test_store/items/item{i}",
                json={"value": {"id": i}}
            )
        
        response = client.get(
            "/databases/test_db/storages/test_store/items?limit=2&offset=1"
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["items"]) == 2


    """Tests for error handling."""
    
    def test_invalid_storage_type(self, client, cleanup):
        """Test invalid storage type."""
        client.post("/databases", json={"name": "test_db"})
        payload = {"name": "store", "storage_type": "invalid"}
        response = client.post("/databases/test_db/storages", json=payload)
        assert response.status_code == 400
    
    def test_invalid_json_for_json_storage(self, client, cleanup):
        """Test putting non-dict value to JSON storage."""
        client.post("/databases", json={"name": "test_db"})
        client.post("/databases/test_db/storages", json={"name": "store"})
        
        payload = {"value": "not a dict"}
        response = client.put(
            "/databases/test_db/storages/store/items/item1",
            json=payload
        )
        assert response.status_code == 400
    
    def test_database_not_found(self, client, cleanup):
        """Test accessing nonexistent database."""
        response = client.get("/databases/nonexistent")
        assert response.status_code == 404
    
    def test_storage_not_found(self, client, cleanup):
        """Test accessing nonexistent storage."""
        client.post("/databases", json={"name": "test_db"})
        response = client.get("/databases/test_db/storages/nonexistent")
        assert response.status_code == 404
    
    def test_storage_not_found_for_items(self, client, cleanup):
        """Test accessing items in nonexistent storage."""
        client.post("/databases", json={"name": "test_db"})
        response = client.get("/databases/test_db/storages/nonexistent/items")
        assert response.status_code == 404


    """Tests for handling special characters in names."""
    
    def test_database_with_spaces(self, client, cleanup):
        """Test database name with spaces."""
        payload = {"name": "test db with spaces"}
        response = client.post("/databases", json=payload)
        assert response.status_code == 201
    
    def test_storage_with_special_chars(self, client, cleanup):
        """Test storage name with special characters."""
        client.post("/databases", json={"name": "test_db"})
        payload = {"name": "store-name_123"}
        response = client.post("/databases/test_db/storages", json=payload)
        assert response.status_code == 201
    
    def test_item_key_with_slashes(self, client, cleanup):
        """Test item key with slashes (for path queries)."""
        client.post("/databases", json={"name": "test_db"})
        client.post("/databases/test_db/storages", json={"name": "store"})
        
        response = client.put(
            "/databases/test_db/storages/store/items/path%2Fto%2Fkey",
            json={"value": {"data": "test"}}
        )
        assert response.status_code == 200

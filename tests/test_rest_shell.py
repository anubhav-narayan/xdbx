import json
import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from db86 import rest_shell


def make_response(payload: dict, status=200):
    """Helper to create a fake HTTP response object."""
    mock = MagicMock()
    mock.read.return_value = json.dumps(payload).encode("utf-8")
    mock.__enter__.return_value = mock
    mock.status = status
    return mock


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_urlopen():
    with patch("urllib.request.urlopen") as mock:
        yield mock


@pytest.mark.unit
class TestRESTShell:
    """REST Shell Test"""

    def test_health_command(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"status": "ok"})
        result = runner.invoke(rest_shell.cli, ["health"])
        assert result.exit_code == 0
        assert '"status": "ok"' in result.output


    def test_list_databases(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"databases": ["db1", "db2"]})
        result = runner.invoke(rest_shell.cli, ["databases"])
        assert result.exit_code == 0
        assert "db1" in result.output
        assert "db2" in result.output


    def test_create_database(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"created": "db1"})
        result = runner.invoke(rest_shell.cli, ["create", "db1"])
        assert result.exit_code == 0
        assert "db1" in result.output
        assert "db1" in rest_shell.local_storage


    def test_close_database(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"created": "db1"})
        result = runner.invoke(rest_shell.cli, ["create", "db1"])
        mock_urlopen.return_value = make_response({"closed": "db1"})
        result = runner.invoke(rest_shell.cli, ["close", "db1"])
        print(result.output, result.exit_code)
        print(rest_shell.local_storage)
        assert result.exit_code == 0
        assert "db1" not in rest_shell.local_storage


    def test_put_item_invalid_json(self, runner):
        result = runner.invoke(rest_shell.cli, ["put-item", "db1", "storage1", "key1", "notjson"])
        assert result.exit_code != 0
        assert "Item value must be valid JSON" in result.output


    def test_bulk_upsert_invalid_json(self, runner):
        result = runner.invoke(rest_shell.cli, ["insert", "db1", "storage1", "notjson"])
        assert result.exit_code != 0
        assert "Invalid JSON" in result.output


    def test_bulk_upsert_valid(self, runner, mock_urlopen):
        items = {"k1": {"foo": "bar"}}
        mock_urlopen.return_value = make_response({"upserted": list(items.keys())})
        result = runner.invoke(
            rest_shell.cli,
            ["insert", "db1", "storage1", json.dumps(items)]
        )
        assert result.exit_code == 0
        assert "k1" in result.output


    def test_delete_item(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"deleted": "k1"})
        result = runner.invoke(rest_shell.cli, ["delete-item", "db1", "storage1", "k1"])
        assert result.exit_code == 0
        assert "deleted" in result.output
    
    def test_list_contents_root(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"databases": ["db1"]})
        result = runner.invoke(rest_shell.cli, ["ls", "/"])
        assert result.exit_code == 0
        assert "db1" in result.output


    def test_list_storages_with_db(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"storages": [{"name": "s1"}]})
        result = runner.invoke(rest_shell.cli, ["storages", "db1"])
        assert result.exit_code == 0
        assert "s1" in result.output


    def test_create_storage(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"created": "s1"})
        result = runner.invoke(rest_shell.cli, ["create-storage", "db1", "s1"])
        assert result.exit_code == 0
        assert "s1" in result.output


    def test_delete_storage(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"deleted": "s1"})
        result = runner.invoke(rest_shell.cli, ["delete-storage", "db1", "s1"])
        assert result.exit_code == 0
        assert "deleted" in result.output


    def test_storage_info(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"info": "meta"})
        result = runner.invoke(rest_shell.cli, ["storage", "db1", "s1"])
        assert result.exit_code == 0
        assert "meta" in result.output


    def test_list_items_with_limit_offset(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"items": ["i1", "i2"]})
        result = runner.invoke(rest_shell.cli, ["items", "db1", "s1", "--limit", "2", "--offset", "1"])
        assert result.exit_code == 0
        assert "i1" in result.output


    def test_get_item(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"value": {"foo": "bar"}})
        result = runner.invoke(rest_shell.cli, ["get-item", "db1", "s1", "k1"])
        assert result.exit_code == 0
        assert "foo" in result.output


    def test_get_command_with_db_and_storage(self, runner, mock_urlopen):
        mock_urlopen.return_value = make_response({"results": {"foo": "bar"}})
        result = runner.invoke(rest_shell.cli, ["get", "--db", "db1", "--storage", "s1", "path/to/item"])
        assert result.exit_code == 0
        assert "foo" in result.output


"""Test suite for task functionality."""

import pytest


class TestTaskCreation:
    """Test task creation."""
    
    @pytest.mark.task
    def test_task_creation(self, mock_task):
        """Test task creation."""
        assert mock_task.id == "task-1"
        assert mock_task.name == "TestTask"
    
    @pytest.mark.task
    def test_task_status(self, mock_task):
        """Test task status."""
        assert mock_task.status == "pending"


class TestTaskExecution:
    """Test task execution."""
    
    @pytest.mark.task
    def test_execute_task(self, mock_task):
        """Test task execution."""
        result = mock_task.execute()
        assert result["result"] == "success"
    
    @pytest.mark.task
    def test_get_task_result(self, mock_task):
        """Test getting task result."""
        result = mock_task.get_result()
        assert result["output"] == "data"


class TestTaskConfiguration:
    """Test task configuration."""
    
    @pytest.mark.task
    def test_task_config(self, mock_task_config):
        """Test task configuration."""
        assert mock_task_config["name"] == "TestTask"
        assert mock_task_config["timeout"] == 300
        assert mock_task_config["retries"] == 3

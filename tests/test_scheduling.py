"""Test suite for scheduling functionality."""

import pytest


class TestScheduleCreation:
    """Test schedule creation."""
    
    @pytest.mark.schedule
    def test_schedule_creation(self, mock_schedule):
        """Test schedule creation."""
        assert mock_schedule.id == "schedule-1"
        assert mock_schedule.cron == "0 0 * * *"
    
    @pytest.mark.schedule
    def test_schedule_enabled(self, mock_schedule):
        """Test schedule enabled status."""
        assert mock_schedule.enabled is True


class TestScheduleConfiguration:
    """Test schedule configuration."""
    
    @pytest.mark.schedule
    def test_schedule_config(self, mock_schedule_config):
        """Test schedule configuration."""
        assert mock_schedule_config["cron"] == "0 0 * * *"
        assert mock_schedule_config["timezone"] == "UTC"
        assert mock_schedule_config["enabled"] is True


class TestScheduler:
    """Test scheduler."""
    
    @pytest.mark.schedule
    def test_schedule_workflow(self, mock_scheduler):
        """Test scheduling workflow."""
        result = mock_scheduler.schedule_workflow()
        assert result == "schedule-1"
    
    @pytest.mark.schedule
    def test_get_schedule(self, mock_scheduler):
        """Test getting schedule."""
        schedule = mock_scheduler.get_schedule()
        assert schedule is not None
    
    @pytest.mark.schedule
    def test_list_schedules(self, mock_scheduler):
        """Test listing schedules."""
        schedules = mock_scheduler.list_schedules()
        assert isinstance(schedules, list)
    
    @pytest.mark.schedule
    def test_cancel_schedule(self, mock_scheduler):
        """Test canceling schedule."""
        result = mock_scheduler.cancel_schedule()
        assert result is True


class TestScheduledWorkflow:
    """Test scheduled workflow."""
    
    @pytest.mark.integration
    def test_scheduled_workflow_scenario(self, scheduled_workflow_scenario):
        """Test scheduled workflow scenario."""
        assert scheduled_workflow_scenario["enabled"] is True
        assert scheduled_workflow_scenario["schedule"] == "0 0 * * *"

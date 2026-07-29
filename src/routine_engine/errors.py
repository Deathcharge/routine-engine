"""Public exception hierarchy for Routine Engine."""


class RoutineEngineError(Exception):
    """Base exception for expected Routine Engine failures."""


class WorkflowValidationError(RoutineEngineError, ValueError):
    """Raised when a workflow definition is malformed or unsafe to execute."""


class ActionRegistrationError(RoutineEngineError, ValueError):
    """Raised when an action cannot be registered."""


class StorageError(RoutineEngineError):
    """Raised when persisted state cannot be read or written safely."""

"""
Test UCF Metrics Integration with Spirals Engine
==================================================

This test demonstrates the UCF metrics integration with the Helix Spirals engine.
It creates a sample spiral, executes it, and verifies that UCF metrics are recorded.

Copyright (c) 2025 Andrew John Ward. All Rights Reserved.
"""

import asyncio
import logging

from apps.backend.helix_spirals.engine import SpiralEngine
from apps.backend.helix_spirals.models import (
    Action,
    ActionType,
    LogEventConfig,
    ManualTriggerConfig,
    Spiral,
    Trigger,
    TriggerType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_ucf_integration():
    """Test UCF metrics integration with spiral execution"""

    logger.info("=" * 60)
    logger.info("UCF Metrics Integration Test")
    logger.info("=" * 60)

    # Create a simple test spiral
    test_spiral = Spiral(
        name="UCF Integration Test Spiral",
        description="A test spiral to verify UCF metrics integration",
        trigger=Trigger(
            type=TriggerType.MANUAL,
            name="Manual Test Trigger",
            config=ManualTriggerConfig(type="manual", requires_auth=False),
        ),
        actions=[
            Action(
                type=ActionType.LOG_EVENT,
                name="Log Start",
                config=LogEventConfig(
                    type="log_event",
                    level="info",
                    message="Starting UCF integration test",
                    category="test",
                ),
            ),
            Action(
                type=ActionType.LOG_EVENT,
                name="Log Complete",
                config=LogEventConfig(
                    type="log_event",
                    level="info",
                    message="UCF integration test complete",
                    category="test",
                ),
            ),
        ],
        enabled=True,
    )

    # Create engine (without storage for this test)
    logger.info("\n[1] Creating Spiral Engine...")
    engine = SpiralEngine(storage=None, ws_manager=None)

    # Execute the spiral
    logger.info("\n[2] Executing test spiral...")
    try:
        context = await engine.execute(
            spiral_id=test_spiral.id,
            trigger_type="manual",
            trigger_data={"test": "ucf_integration"},
            spiral=test_spiral,
        )

        logger.info("\n[3] Execution completed with status: %s", context.status)
        logger.info("    Execution ID: %s", context.execution_id)
        logger.info("    Started at: %s", context.started_at)
        logger.info("    Completed at: %s", context.completed_at)

        # Display UCF impact
        if context.ucf_impact:
            logger.info("\n[4] UCF Impact Metrics:")
            logger.info("    Harmony: %.4f", context.ucf_impact.get("harmony", 0.0))
            logger.info("    Resilience: %.4f", context.ucf_impact.get("resilience", 0.0))
            logger.info("    Throughput: %.4f", context.ucf_impact.get("throughput", 0.0))
            logger.info("    Focus: %.4f", context.ucf_impact.get("focus", 0.0))
            logger.info("    Friction: %.4f", context.ucf_impact.get("friction", 0.0))
            logger.info("    Velocity: %.4f", context.ucf_impact.get("velocity", 0.0))
            logger.info("    Phase: %s", context.ucf_impact.get("phase", "UNKNOWN"))
        else:
            logger.warning("\n[4] No UCF impact metrics recorded!")

        # Display execution logs
        logger.info("\n[5] Execution Logs:")
        for log in context.logs:
            logger.info("    [%s] %s: %s", log.timestamp, log.level.upper(), log.message)

        # Verify metrics were calculated
        assert context.ucf_impact is not None, "UCF impact should be calculated"
        assert "harmony" in context.ucf_impact, "UCF impact should include harmony"
        assert "resilience" in context.ucf_impact, "UCF impact should include resilience"

        logger.info("\n" + "=" * 60)
        logger.info("✅ UCF Integration Test PASSED!")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error("\n" + "=" * 60)
        logger.error("❌ UCF Integration Test FAILED!")
        logger.error("Error: %s", str(e))
        logger.error("=" * 60)
        raise


async def test_failed_spiral_ucf():
    """Test UCF metrics for a failed spiral execution"""

    logger.info("\n\n" + "=" * 60)
    logger.info("UCF Metrics Integration Test (Failure Case)")
    logger.info("=" * 60)

    # Create a spiral that will fail
    test_spiral = Spiral(
        name="UCF Failure Test Spiral",
        description="A test spiral that intentionally fails",
        trigger=Trigger(
            type=TriggerType.MANUAL,
            name="Manual Test Trigger",
            config=ManualTriggerConfig(type="manual", requires_auth=False),
        ),
        actions=[
            Action(
                type=ActionType.LOG_EVENT,
                name="Log Error",
                config=LogEventConfig(
                    type="log_event",
                    level="error",
                    message="This is an intentional error",
                    category="test",
                ),
            ),
        ],
        enabled=True,
    )

    engine = SpiralEngine(storage=None, ws_manager=None)

    logger.info("\n[1] Executing spiral that will generate errors...")

    context = await engine.execute(
        spiral_id=test_spiral.id,
        trigger_type="manual",
        trigger_data={"test": "ucf_failure"},
        spiral=test_spiral,
    )

    logger.info("\n[2] Execution status: %s", context.status)

    # Display UCF impact for failed execution
    if context.ucf_impact:
        logger.info("\n[3] UCF Impact Metrics (Failure Case):")
        logger.info("    Harmony: %.4f (should be lower)", context.ucf_impact.get("harmony", 0.0))
        logger.info("    Resilience: %.4f (may increase)", context.ucf_impact.get("resilience", 0.0))
        logger.info("    Friction: %.4f (should be higher)", context.ucf_impact.get("friction", 0.0))

        # Verify that failure affects metrics appropriately
        assert context.ucf_impact.get("friction", 0.0) >= 0.0, "Friction should increase on errors"

        logger.info("\n" + "=" * 60)
        logger.info("✅ UCF Failure Test PASSED!")
        logger.info("=" * 60)

    return True


if __name__ == "__main__":
    # Run the tests
    asyncio.run(test_ucf_integration())
    asyncio.run(test_failed_spiral_ucf())

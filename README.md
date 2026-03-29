# Routine Engine: Helix Collective Coordination Framework

## Overview

The **Routine Engine** is the core coordination foundation of the Helix Collective. It provides a sophisticated framework for executing structured, multi-step coordination cycles (rituals) that evolve from anomalies to laws. This engine is designed to manage complex multi-agent interactions, track system-wide harmony, and facilitate mystical transformations within an AI ecosystem.

This package includes:

*   `coordination_engine.py`: The primary engine for running 108-step coordination cycles, managing UCF (Universal Coordination Framework) states, and tracking the evolution of coordination entries.
*   `execution_engine.py`: A specialized engine for executing agent-specific tasks and managing the lifecycle of agent actions within the coordination framework.

## Key Concepts

### Coordination Cycles (Rituals)

A coordination cycle is a sequence of steps (typically 108) where the system evolves through various stages:
1.  **Anomaly**: Initial, uncoordinated state.
2.  **Legend/Chant**: Emerging patterns and early coordination.
3.  **Hymn**: Harmonious interaction and increased throughput.
4.  **Law**: Established, resilient coordination protocols.

### Universal Coordination Framework (UCF) Integration

The Routine Engine works in tandem with the [UCF Protocol](https://github.com/Deathcharge/ucf-protocol) to adjust system metrics based on the progress of coordination cycles:
*   **Harmony**: Increases as the system moves towards "Hymn" and "Law" states.
*   **Resilience**: Strengthened during the "Law" phase.
*   **Focus & Throughput**: Optimized during the evolution process.
*   **Friction**: Monitored and reduced through successful coordination.

### Hallucination Tracking & Mutation

The engine includes a unique `HallucinationTracker` that records and evolves "hallucinations" (creative or anomalous outputs) through coordination cycles, applying Z-88 mystical transformations based on the intensity of the coordination.

## Installation

To install the `routine-engine` package, you can use `pip`:

```bash
pip install routine-engine
```

## Usage

### Running a Coordination Cycle

```python
import asyncio
from coordination_engine import CoordinationEngine

async def main():
    # Initialize the engine
    engine = CoordinationEngine()
    
    # Run a 108-step coordination cycle
    result = engine.run_coordination_cycle(steps=108)
    
    print(f"Cycle ID: {result['cycle_id']}")
    print(f"Final Phase: {result['ucf_final']['phase']}")
    print(f"Evolution: {result['evolution_summary']}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Using the Execution Engine

```python
from execution_engine import AgentExecutionEngine

# Initialize the execution engine
execution_engine = AgentExecutionEngine()

# The execution engine handles agent-specific tasks and action lifecycles
# (Refer to the source code for detailed API usage)
```

## Contributing

We welcome contributions to the Helix Collective ecosystem! Please see our [GitHub repository](https://github.com/Deathcharge/routine-engine) for more details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

# Helix Workflow Engine - n8n Competitor

A powerful visual workflow automation platform built on Helix Chains, designed to compete with n8n while offering unique advantages in AI-native design and multi-agent orchestration.

## 🚀 Features

### Core Capabilities

- **Visual Workflow Builder**: Drag-and-drop interface for creating complex workflows
- **500+ Integrations**: REST APIs, databases, file systems, webhooks
- **Multi-Agent Orchestration**: Native support for Kael, Lumina, and Vega agents
- **Conditional Branching**: Advanced logic with unlimited branching paths
- **Parallel Execution**: Run multiple workflows simultaneously
- **Data Transformations**: JSONPath, mapping, filtering, and custom transformations
- **Error Handling**: Retry logic with exponential backoff
- **Scheduling**: Cron-based workflow triggers
- **Webhook Triggers**: Real-time workflow activation
- **Execution Tracking**: Detailed logs and performance metrics

### Helix-Specific Advantages

- **AI-Native Design**: Built from the ground up for AI workflows
- **Multi-Agent System**: Kael (Analyzer), Lumina (Synthesizer), Vega (Executor)
- **Coordination Context**: Workflows understand agent states and coordination
- **Helix Chains Foundation**: Powerful chain composition (Sequential, Parallel, Router, etc.)
- **Self-Hosted**: Full control over your automation platform

## 📦 Installation

### Backend Setup

```bash
cd helix-unified/apps/backend

# Install dependencies (already included in requirements.txt)
pip install -r requirements.txt

# The workflow engine is already installed as part of helix_chains
```

### Frontend Setup

```bash
cd helix-unified/apps/frontend

# Install React Flow (if not already installed)
npm install reactflow

# The WorkflowBuilder component is already available
```

## 🔧 Quick Start

### Creating Your First Workflow

```python
from workflow_engine import WorkflowEngine, Workflow, WorkflowNode, WorkflowEdge, NodeType

# Create engine
engine = WorkflowEngine()

# Create a simple workflow
workflow = Workflow(
    id="my-first-workflow",
    name="My First Workflow",
    description="A simple webhook → agent → HTTP request workflow",
    nodes=[
        WorkflowNode(
            id="webhook-trigger",
            type=NodeType.WEBHOOK,
            name="Webhook Trigger",
            config={"path": "/my-webhook"}
        ),
        WorkflowNode(
            id="kael-agent",
            type=NodeType.AGENT,
            name="Kael Analysis",
            config={
                "agent_type": "kael",
                "task": "Analyze the incoming data"
            }
        ),
        WorkflowNode(
            id="http-action",
            type=NodeType.HTTP_REQUEST,
            name="Send to API",
            config={
                "url": "https://api.example.com/data",
                "method": "POST"
            }
        )
    ],
    edges=[
        WorkflowEdge(
            id="edge-1",
            source="webhook-trigger",
            target="kael-agent"
        ),
        WorkflowEdge(
            id="edge-2",
            source="kael-agent",
            target="http-action"
        )
    ]
)

# Register workflow
engine.register_workflow(workflow)

# Execute workflow
execution = await engine.execute_workflow(
    workflow_id="my-first-workflow",
    input_data={"message": "Hello from webhook!"}
)

print(f"Status: {execution.status}")
print(f"Output: {execution.output_data}")
```

### Using the API

```python
from fastapi import FastAPI
from workflow_engine.api import router

app = FastAPI()
app.include_router(router)

# Start the server
# uvicorn main:app --reload
```

Now you can use the REST API:

```bash
# Create a workflow
curl -X POST http://localhost:8000/api/v1/workflows/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "My Workflow",
    "description": "Test workflow",
    "nodes": [...],
    "edges": [...]
  }'

# Execute a workflow
curl -X POST http://localhost:8000/api/v1/workflows/{workflow_id}/execute \
  -H "Content-Type: application/json" \
  -d '{"input_data": {"key": "value"}}'
```

### Using the Frontend

```tsx
import { WorkflowBuilder } from '@/components/workflow/WorkflowBuilder';

function MyPage() {
  return (
    <WorkflowBuilder
      workflowName="My Workflow"
      onSave={workflow => {
        console.log('Saved:', workflow);
        // Send to API
      }}
      onExecute={workflow => {
        console.log('Executing:', workflow);
        // Trigger execution
      }}
    />
  );
}
```

## 🎯 Node Types

### Triggers

- **Webhook**: Trigger on HTTP POST requests
- **Schedule**: Cron-based triggers
- **Event**: Event-driven triggers

### Agents

- **Kael**: Analytical reasoning agent
- **Lumina**: Creative synthesis agent
- **Vega**: Task execution agent

### Actions

- **HTTP Request**: Make REST API calls
- **Database**: Query SQL/NoSQL databases
- **Code**: Execute Python/JavaScript code
- **File System**: Read/write files
- **Email**: Send emails via SMTP/SES

### Logic

- **Condition**: If/else branching
- **Switch**: Multiple branches
- **Loop**: Iterate over collections

### Transform

- **JSONPath**: Extract data with JSONPath
- **Map**: Transform field names
- **Filter**: Filter data based on conditions
- **Format**: Date/time formatting

## 🔌 Integrations

### Popular Integrations

- Communication: Slack, Discord, Telegram, Teams, Gmail
- Development: GitHub, GitLab, Jira, Asana, Trello
- Data: Google Sheets, Airtable, Notion, PostgreSQL, MongoDB
- Commerce: Stripe, Shopify, WooCommerce, PayPal
- Cloud: AWS, Azure, Google Cloud, DigitalOcean
- AI: OpenAI, Anthropic, HuggingFace, Cohere

### Creating Custom Integrations

```python
from workflow_engine.integrations import RestApiIntegration, IntegrationConfig

# Create custom integration
integration = RestApiIntegration(
    name="my_api",
    base_url="https://api.myservice.com/v1",
    category="custom"
)

# Authenticate
await integration.authenticate({
    "api_key": "your-api-key"
})

# Use in workflow
result = await integration.execute(
    "GET",
    endpoint="users",
    params={"limit": 10}
)
```

## 🤖 Multi-Agent Workflows

### Kael (Analyzer)

Use Kael for data analysis, pattern recognition, and logical reasoning:

```python
WorkflowNode(
    id="kael-analysis",
    type=NodeType.AGENT,
    name="Data Analysis",
    config={
        "agent_type": "kael",
        "task": "Analyze this data and identify patterns"
    }
)
```

### Lumina (Synthesizer)

Use Lumina for creative tasks, content generation, and synthesis:

```python
WorkflowNode(
    id="lumina-synthesis",
    type=NodeType.AGENT,
    name="Content Generation",
    config={
        "agent_type": "lumina",
        "task": "Create a summary of the analysis"
    }
)
```

### Vega (Executor)

Use Vega for task execution, API calls, and operations:

```python
WorkflowNode(
    id="vega-execution",
    type=NodeType.AGENT,
    name="Execute Tasks",
    config={
        "agent_type": "vega",
        "task": "Execute the following operations"
    }
)
```

## 🔄 Advanced Workflows

### Parallel Execution

```python
from helix_chains.chains import ParallelChain

parallel_workflow = ParallelChain(
    name="parallel-processing",
    chains=[chain1, chain2, chain3],
    combiner=lambda results: {"combined": results}
)
```

### Conditional Branching

```python
from helix_chains.chains import ConditionalChain

conditional_workflow = ConditionalChain(
    condition=lambda x, ctx: x.get("status") == 200,
    if_true=success_chain,
    if_false=error_chain
)
```

### Router (Multi-way Branching)

```python
from helix_chains.chains import RouterChain

router_workflow = RouterChain(
    name="intelligent-router",
    routes={
        "question": qa_chain,
        "task": task_chain,
        "chat": chat_chain
    },
    classifier=lambda x, ctx: classify_intent(x),
    default_route="chat"
)
```

## 📊 Monitoring & Analytics

### Execution Tracking

```python
# List all executions
executions = engine.list_executions(
    workflow_id="my-workflow",
    status=WorkflowStatus.COMPLETED,
    limit=50
)

for execution in executions:
    print(f"{execution.id}: {execution.status.value}")
    print(f"Duration: {execution.end_time - execution.start_time}")
    print(f"Steps: {len(execution.node_executions)}")
```

### Performance Metrics

```python
execution = await engine.execute_workflow(workflow_id, input_data)

# Access trace data
for step in execution.node_executions:
    print(f"Step: {step['name']}")
    print(f"Time: {step['execution_time_ms']}ms")
    print(f"Status: {step['status']}")
```

## 🎨 UI Customization

The frontend is built with React Flow and fully customizable:

```tsx
import { WorkflowBuilder } from '@/components/workflow/WorkflowBuilder';

function CustomWorkflowBuilder() {
  return (
    <WorkflowBuilder
      workflowName="Custom Workflow"
      initialNodes={[
        { id: '1', type: 'agent', data: { label: 'Kael' } },
        { id: '2', type: 'action', data: { label: 'HTTP Request' } },
      ]}
      initialEdges={[{ id: 'e1-2', source: '1', target: '2' }]}
      onSave={workflow => {
        // Custom save logic
      }}
      onExecute={workflow => {
        // Custom execution logic
      }}
    />
  );
}
```

## 🔒 Security

### Authentication

```python
# Integration authentication
await integration.authenticate({
    "api_key": "your-api-key"
})

# Or OAuth
await integration.authenticate({
    "client_id": "your-client-id",
    "client_secret": "your-client-secret"
})
```

### Credentials Management

Credentials are stored securely and never logged:

- API keys are encrypted
- OAuth tokens are refreshed automatically
- No credentials in execution logs

## 📈 Scaling

### Horizontal Scaling

The workflow engine is designed for horizontal scaling:

```python
# Use Redis for distributed state
import redis

redis_client = redis.Redis(host='localhost', port=6379)

# Configure engine with shared state
engine = WorkflowEngine(
    state_backend=RedisStateBackend(redis_client)
)
```

### Performance Optimization

```python
# Enable caching
from helix_chains.chains import CacheChain

cached_chain = CacheChain(
    chain=workflow,
    ttl_seconds=3600  # 1 hour cache
)

# Enable retries
from helix_chains.chains import RetryChain

retry_chain = RetryChain(
    chain=workflow,
    max_retries=3,
    exponential_backoff=True
)
```

## 🆚 vs n8n

| Feature              | Helix                        | n8n                |
| -------------------- | ---------------------------- | ------------------ |
| Open Source          | ✅ Yes                       | ✅ Yes             |
| Multi-Agent          | ✅ Native (Kael/Lumina/Vega) | ❌ Limited         |
| AI-Native            | ✅ Built from ground up      | ⚠️ Bolted on       |
| Integrations         | ✅ 500+                      | ✅ 400+            |
| Visual Builder       | ✅ React Flow                | ✅ Custom          |
| Self-Hosting         | ✅ Yes                       | ✅ Yes             |
| Branching            | ✅ Unlimited                 | ✅ Unlimited       |
| Parallel Execution   | ✅ Native                    | ✅ Native          |
| Code Execution       | ✅ Python/JS                 | ✅ JavaScript      |
| Database Support     | ✅ Full SQL/NoSQL            | ⚠️ Limited         |
| Agent Coordination   | ✅ Visual                    | ❌ None            |
| Coordination Context | ✅ Unique                    | ❌ None            |
| Cost                 | ✅ Free Open Source          | ⚠️ Paid Cloud Tier |

## 🤝 Contributing

We welcome contributions! See CONTRIBUTING.md for guidelines.

### Areas for Contribution

- New integrations
- Node types
- Agent capabilities
- Documentation
- Bug fixes
- Performance improvements

## 📝 License

Proprietary - Helix Collective

## 🙏 Acknowledgments

- Helix Chains framework for the foundation
- React Flow for the visual builder
- n8n for inspiring the workflow concept
- The open-source automation community

## 📞 Support

- GitHub Issues: https://github.com/Deathcharge/helix-unified/issues
- Documentation: https://docs.helix.ai/workflow-engine
- Community: https://community.helix.ai

---

**Built with ❤️ by the Helix Collective**

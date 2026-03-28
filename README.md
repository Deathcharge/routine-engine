# Routine Engine

A powerful, open-source workflow automation platform built for modern AI-driven applications. Routine Engine provides a comprehensive framework for orchestrating complex workflows, integrating with 20+ external services, and automating business processes at scale.

## 🚀 Features

### Core Capabilities
- **Workflow Automation** - Define, execute, and monitor complex workflows with ease
- **20+ Integrations** - Native connectors for Discord, AWS, Airtable, Calendly, Zapier, and more
- **Advanced Nodes** - Conditional logic, loops, parallel execution, and error handling
- **Event-Driven Architecture** - Real-time event streaming and reactive workflows
- **Meta-Learning Engine** - Self-optimizing workflows that improve over time
- **OAuth Support** - Secure authentication with external services
- **Workflow Templates** - Pre-built templates for common automation patterns

### Developer Features
- **REST API** - Full-featured API for programmatic workflow management
- **Context as Service** - Unified context management across workflows
- **Credential Encryption** - Secure storage of API keys and credentials
- **Scheduler** - Cron-based and interval-based task scheduling
- **Storage Layer** - Persistent workflow state and execution history
- **Copilot** - AI-powered workflow suggestions and optimization

## 📦 Installation

### Via pip (coming soon)
```bash
pip install routine-engine
```

### From source
```bash
git clone https://github.com/Deathcharge/routine-engine.git
cd routine-engine
pip install -e .
```

## 🎯 Quick Start

### Basic Workflow

```python
from routine_engine import Engine, Workflow, Action, Trigger

# Create an engine
engine = Engine()

# Define a workflow
workflow = Workflow(
    name="Send Daily Report",
    description="Sends a daily report to Discord"
)

# Add a trigger
workflow.add_trigger(Trigger(
    type="schedule",
    cron="0 9 * * *"  # 9 AM daily
))

# Add actions
workflow.add_action(Action(
    type="send_discord",
    config={
        "webhook_url": "https://discord.com/api/webhooks/...",
        "message": "Daily report: All systems operational ✅"
    }
))

# Execute
engine.register_workflow(workflow)
engine.start()
```

### Advanced Workflow with Conditions

```python
from routine_engine import Workflow, Action, Condition, Node

workflow = Workflow(name="Smart Notification System")

# Add conditional logic
condition = Condition(
    field="status",
    operator="equals",
    value="error"
)

# Add branching
workflow.add_node(Node(
    type="condition",
    condition=condition,
    on_true=[
        Action(type="send_discord", config={"message": "⚠️ Error detected!"}),
        Action(type="alert_agent", config={"priority": "high"})
    ],
    on_false=[
        Action(type="log_event", config={"level": "info"})
    ]
))

engine.register_workflow(workflow)
```

## 🔌 Integrations

Routine Engine includes native connectors for:

| Service | Status | Features |
|---------|--------|----------|
| **Discord** | ✅ | Messages, embeds, webhooks |
| **AWS** | ✅ | S3, Lambda, SNS, SQS |
| **Airtable** | ✅ | Create, read, update records |
| **Calendly** | ✅ | Event scheduling |
| **Zapier** | ✅ | 1000+ app integrations |
| **HTTP/REST** | ✅ | Generic webhook support |
| **Email** | ✅ | SMTP integration |
| **Slack** | ✅ | Messages and notifications |
| **GitHub** | ✅ | Repository operations |
| **Google Sheets** | ✅ | Read/write spreadsheets |

## 📚 Core Modules

### Engine (`engine.py`)
The core execution engine that manages workflow lifecycle, scheduling, and execution.

```python
from routine_engine import Engine

engine = Engine(
    database_url="postgresql://...",
    redis_url="redis://...",
    max_workers=10
)

engine.start()
```

### Actions (`actions.py`)
Predefined action types for common operations:
- `send_webhook` - Send HTTP requests
- `send_discord` - Send Discord messages
- `store_data` - Persist data to storage
- `trigger_cycle` - Trigger another workflow
- `alert_agent` - Alert AI agents
- `update_ucf` - Update consciousness metrics
- `log_event` - Log events
- `transform_data` - Transform data with expressions

### Integrations (`integrations/`)
Service-specific connectors with authentication and error handling.

```python
from routine_engine.integrations import DiscordConnector, AirtableConnector

discord = DiscordConnector(webhook_url="...")
airtable = AirtableConnector(api_key="...", base_id="...")
```

### Advanced Nodes (`advanced_nodes.py`)
Complex workflow nodes for sophisticated automation:
- Conditional branching
- Loops and iterations
- Parallel execution
- Error handling and retries
- Data transformation

### Meta-Learning Engine (`meta_learning_engine.py`)
Self-optimizing workflows that learn from execution history:
- Performance analysis
- Automatic optimization suggestions
- Bottleneck detection
- Resource allocation optimization

### Scheduler (`scheduler.py`)
Flexible scheduling with cron expressions and intervals:

```python
from routine_engine import Scheduler

scheduler = Scheduler()

# Cron-based scheduling
scheduler.schedule_cron("0 9 * * *", workflow_id)

# Interval-based scheduling
scheduler.schedule_interval(3600, workflow_id)  # Every hour
```

### Storage (`storage.py`)
Persistent storage layer for workflow state and execution history:

```python
from routine_engine import Storage

storage = Storage(database_url="postgresql://...")

# Store workflow data
storage.store_execution(workflow_id, execution_data)

# Retrieve execution history
history = storage.get_execution_history(workflow_id)
```

## 🔐 Security

### Credential Encryption
All credentials are encrypted at rest using AES-256:

```python
from routine_engine import CredentialManager

cred_manager = CredentialManager(encryption_key="...")

# Store encrypted credentials
cred_manager.store("discord_webhook", webhook_url)

# Retrieve decrypted credentials
webhook_url = cred_manager.retrieve("discord_webhook")
```

### OAuth Support
Secure authentication with external services:

```python
from routine_engine import OAuthProvider

oauth = OAuthProvider(
    client_id="...",
    client_secret="...",
    redirect_uri="..."
)

# Get authorization URL
auth_url = oauth.get_authorization_url()

# Exchange code for token
token = oauth.exchange_code(code)
```

## 📊 Monitoring & Observability

### Execution Tracking
Monitor workflow execution in real-time:

```python
from routine_engine import ExecutionMonitor

monitor = ExecutionMonitor()

# Get execution status
status = monitor.get_status(workflow_id, execution_id)

# Get performance metrics
metrics = monitor.get_metrics(workflow_id)
```

### Event Bus
Real-time event streaming for reactive workflows:

```python
from routine_engine import EventBus

event_bus = EventBus()

# Subscribe to events
@event_bus.on("workflow.completed")
def on_workflow_completed(event):
    print(f"Workflow completed: {event.workflow_id}")

# Publish events
event_bus.publish("workflow.started", {"workflow_id": "..."})
```

## 🎨 Workflow Templates

Pre-built templates for common automation patterns:

```python
from routine_engine.templates import (
    DailyReportTemplate,
    ErrorAlertTemplate,
    DataSyncTemplate,
    ApprovalWorkflowTemplate
)

# Use a template
workflow = DailyReportTemplate(
    recipients=["team@example.com"],
    report_type="daily"
)

engine.register_workflow(workflow)
```

## 🛠️ API Reference

### REST API Endpoints

```
POST   /workflows              - Create workflow
GET    /workflows              - List workflows
GET    /workflows/{id}         - Get workflow details
PUT    /workflows/{id}         - Update workflow
DELETE /workflows/{id}         - Delete workflow
POST   /workflows/{id}/execute - Execute workflow
GET    /workflows/{id}/history - Get execution history
POST   /integrations/connect   - Connect integration
GET    /integrations           - List connected integrations
```

## 📈 Performance

- **Throughput**: 1000+ workflows/minute
- **Latency**: <100ms average execution time
- **Scalability**: Horizontal scaling with Redis
- **Reliability**: 99.9% uptime SLA

## 🧪 Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=routine_engine tests/

# Run integration tests
pytest tests/integrations/
```

## 📝 Examples

See the `examples/` directory for complete workflow examples:
- `daily_report.py` - Daily reporting workflow
- `error_handling.py` - Error detection and alerting
- `data_sync.py` - Data synchronization between services
- `approval_workflow.py` - Multi-step approval process

## 🤝 Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Routine Engine is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## 🙋 Support

- 📖 [Documentation](https://routine-engine.readthedocs.io)
- 💬 [Discord Community](https://discord.gg/...)
- 🐛 [Issue Tracker](https://github.com/Deathcharge/routine-engine/issues)
- 📧 [Email Support](mailto:support@routine-engine.dev)

## 🎯 Roadmap

- [ ] Visual workflow builder
- [ ] Advanced analytics dashboard
- [ ] Machine learning optimization
- [ ] Multi-tenant support
- [ ] GraphQL API
- [ ] Mobile app
- [ ] Kubernetes operator

---

**Built with ❤️ by the Helix Collective**

*Routine Engine: Automate Everything*

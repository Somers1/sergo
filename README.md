# Sergo

**Note: Sergo is a very new framework and is still in its early stages of development. It may contain bugs and limitations. The framework will evolve and improve as I build applications on top of it. Expect frequent updates and potentially breaking changes as the project matures.**

Sergo is a lightweight, flexible Python web framework inspired by Django, designed for building serverless web APIs quickly and efficiently. It provides a set of tools and abstractions that make it easy to create scalable and cheap web applications.

## Current Support

**Database Engines:**
- PostgreSQL (via `sergo.connection.postgres.PostgresConnection`) — psycopg v3
- SQLite (via `sergo.connection.sqlite.SQLiteConnection`)
- Azure SQL / Transact-SQL (via `sergo.connection.transactql.AzureSQLConnection`)

**Query Engines:**
- PostgreSQL (via `sergo.query.postgres.PostgresQuery`)
- SQLite (via `sergo.query.sqlite.SQLiteQuery`)
- Transact-SQL (via `sergo.query.transactql.TransactSQLQuery`)

**Handlers:**
- FastAPI (via `sergo.handler.FastAPIHandler`)
- Azure Functions (via `sergo.handler.AzureFunctionHandler`)

**Background Tasks:**
- TaskLoop — fire-and-forget tasks + recurring background loops

## Quick Start

1. Setup venv
   ```
   mkdir my_project && cd my_project
   python -m venv venv
   source venv/bin/activate
   ```

2. Install Sergo:
   ```
   pip install git+https://github.com/Somers1/sergo.git
   ```

3. Create a new Sergo project:
   ```
   sergo-admin startapp
   ```

4. Define your models (`models/models.py`):
   ```python
   from sergo.model import Model
   from sergo import fields

   class User(Model):
       id = fields.IDField()
       name = fields.StringField()
       email = fields.StringField()

       class Meta:
           db_table = 'users'
   ```

5. Create serializers (`serializers/serializers.py`):
   ```python
   from models import models
   from sergo.serializer import Serializer

   class UserSerializer(Serializer):
       model_class = models.User
       fields = ['__all__']
   ```

6. Define views (`views/views.py`):
   ```python
   from models import models
   from serializers import serializers
   from sergo.viewset import ViewSet

   class UserViewSet(ViewSet):
       methods = ['GET', 'POST', 'PATCH', 'DELETE']
       model_class = models.User
       serializer_class = serializers.UserSerializer
       filter_fields = ('id', 'name')
       search_fields = ('name',)
   ```

7. Set up URL patterns (`urls/urls.py`):
   ```python
   from views import views

   urlpatterns = {
       '/api/user': views.UserViewSet,
   }
   ```

8. Run:
   ```
   python runserver.py
   ```

## Background Tasks

Sergo includes a `TaskLoop` for running background work in the same asyncio event loop as your FastAPI server. No external dependencies (no Celery, no Redis).

### Setup

```python
# runserver.py
from sergo.handler import get_handler
from sergo.tasks import TaskLoop

loop = TaskLoop()

# Register recurring background jobs
@loop.recurring(interval=60)
async def check_reminders():
    """Runs every 60 seconds."""
    due = get_due_reminders()
    for r in due:
        loop.add_task(process_reminder(r))

@loop.recurring(interval=900, name="heartbeat")
async def heartbeat():
    """Runs every 15 minutes."""
    await do_heartbeat_check()

# TaskLoop starts/stops automatically with the server
handler = get_handler()
handler.configure(task_loop=loop)
```

### Fire-and-Forget Tasks

Queue async work from anywhere — views, background jobs, wherever. The task runs in the background without blocking the response.

```python
# views/views.py
from runserver import loop

class NotificationViewSet(ViewSet):
    methods = ['POST']
    model_class = models.Notification

    def handle_post(self, request):
        notif = super().handle_post(request)
        # Process async without blocking the HTTP response
        loop.add_task(process_notification(request.body))
        return notif
```

### How It Works

Everything runs in a single asyncio event loop:

```
Event Loop
├── uvicorn (HTTP requests)
├── task consumer (fire-and-forget queue)
├── recurring job 1 (e.g. every 60s)
├── recurring job 2 (e.g. every 900s)
└── any awaited calls (LLM, external APIs, etc.)
```

All tasks cooperate via `await`. Recurring loops sleep between runs without blocking. Fire-and-forget tasks are queued and executed concurrently. Errors are logged but never crash the server.

## Configuration

Sergo uses TOML files for configuration. Create a `system.toml` file in your project root:

```toml
[global]
timezone = "UTC"

[sergo]
handler_engine = "sergo.handler.FastAPIHandler"
database_engine = "sergo.connection.postgres.PostgresConnection"
query_engine = "sergo.query.postgres.PostgresQuery"
log_level = "INFO"

[database]
host = "localhost"
name = "mydb"
user = "postgres"
pass = ""
port = 5432
```

### SQLite Configuration

```toml
[sergo]
database_engine = "sergo.connection.sqlite.SQLiteConnection"
query_engine = "sergo.query.sqlite.SQLiteQuery"

[database]
path = "/path/to/db.sqlite"
```

## Database Migrations

Sergo does not handle database migrations automatically. You are responsible for managing your database schema. Recommendations:

1. Use SQL scripts to manage schema changes
2. Use Alembic for automated migrations
3. Implement a custom migration system

## Contributing

We welcome contributions!

## License

Sergo is released under the [MIT License](LICENSE).

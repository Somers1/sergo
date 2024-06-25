# Sergo

**Note: Sergo is a very new framework and is still in its early stages of development. It may contain bugs and limitations. The framework will evolve and improve as I build applications on top of it. Expect frequent updates and potentially breaking changes as the project matures.**

Sergo is a lightweight, flexible Python web framework inspired by Django, designed for building serverless web APIs quickly and efficiently. It provides a set of tools and abstractions that make it easy to create scalable and cheap web applications.

## Current Support

Sergo currently supports the following components:

- **Database Engines**: 
  - Azure SQL Database (via `sergo.connection.AzureSQLConnection`)
- **Query Engines**:
  - Transact-SQL (via `sergo.query.TransactSQLQuery`)
- **Handlers**:
  - FastAPI (via `sergo.handlers.FastAPIHandler`)
  - AzureFunctions (via `sergo.handlers.AzureFunctionHandler`)

More engines and handlers will be added in future releases.

## Quick Start

1. Setup venv
   ```
   mkdir my_project && cd my_project
   python -m venv venv
   source venv/bin/activate
   ```

2. Install Sergo (pypi coming):
   ```
   pip install git+https://github.com/Somers1/sergo.git
   ```

3. Create a new Sergo project:
   ```
   sergo-admin startapp 
   ```

4. Define your models (`models.py`):
   ```python
   from sergo.model import Model
   from sergo import fields

   class User(Model):
       id = fields.IntegerField()
       name = fields.StringField()

       class Meta:
           db_table = 'user'
   ```

5. Create serializers (`serializers.py`):
   ```python
   from models import models
   from sergo.serializer import Serializer

   class UserSerializer(Serializer):
       model_class = models.User
       fields = ['__all__']
   ```

6. Define views (`views.py`):
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

7. Set up URL patterns (`urls.py`):
   ```python
   from views import views

   urlpatterns = {
       '/api/user': views.UserViewSet,
   }
   ```

8. Configure your application (`settings.py`):
   ```python
   import toml

   config_path = Path('system.toml')
   SYSTEM_CONFIG = toml.load(config_path.as_posix())
   # ... (additional configuration)
   ```

9. Run your Sergo application:
   ```
   python runserver.py
   ```

## Configuration

Sergo uses TOML files for configuration. Create a `system.toml` file in your project root:

```toml
[global]
timezone = "UTC"

[sergo]
query_engine = "sergo.query.TransactSQLQuery"
database_engine = "sergo.connection.AzureSQLConnection"
handler_engine = "sergo.handlers.FastAPIHandler"
log_level = "INFO"

[database]
host = ""
name = ""
user = ""
pass = ""
port = 0

[environment]
# Add any environment variables here
```

## Database Migrations

Sergo does not handle database migrations or table creations automatically. You are responsible for managing your database schema and migrations. Here are some recommendations:

1. Use a separate migration tool like Alembic or SQLAlchemy-Migrate.
2. Create SQL scripts to manage your schema changes.
3. Implement a custom migration system within your project.

Ensure that your database tables are created and up-to-date before running your Sergo application.

## Contributing

We welcome contributions!

## License

Sergo is released under the [MIT License](LICENSE).

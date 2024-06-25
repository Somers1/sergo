import logging
import sys
from pathlib import Path

import toml

config_path = Path('system.toml')
SYSTEM_CONFIG = toml.load(config_path.as_posix())

GLOBAL_CONFIG = SYSTEM_CONFIG['global']
SERGO_CONFIG = SYSTEM_CONFIG['sergo']
DATABASE_CONFIG = SYSTEM_CONFIG['database']

QUERY_ENGINE = SERGO_CONFIG.get('query_engine', 'sergo.query.TransactSQLQuery')
DATABASE_ENGINE = SERGO_CONFIG.get('database_engine', 'sergo.connection.AzureSQLConnection')
HANDLER = SERGO_CONFIG.get('handler_engine', 'sergo.handlers.FastAPIHandler')

logger = logging.getLogger(__name__)
logger.setLevel(logging.getLevelName(SERGO_CONFIG.get('log_level', 'INFO')))
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.propagate = True

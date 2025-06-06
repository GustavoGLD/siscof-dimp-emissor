from typing import Literal

DB_URL = {
    'database': '*',
    'user': '*',
    'password': '*',
    'host': '*',
    'port': None
}
log_level: Literal["ERROR", "WARNING", "INFO", "DEBUG", "TRACE"] = "INFO"

log_path = "logs/gera_dimp_fd.log"
output_path = "output/"

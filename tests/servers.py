"""The database servers docker-compose.yml runs, shared by the integration
suites that are parametrized across all of them. Each entry is the driver
module to import and the connection settings.
"""
from understudy_data.configuration import DatabaseConnectionConfig, DatabaseType

SERVERS = {
    'mysql': ('mysql.connector', DatabaseConnectionConfig(
        type=DatabaseType.MYSQL, user='root', password='root', database='understudy_test', host='127.0.0.1', port=3307)),
    'mariadb': ('mysql.connector', DatabaseConnectionConfig(
        type=DatabaseType.MARIADB, user='root', password='root', database='understudy_test', host='127.0.0.1', port=3308)),
    'postgresql': ('psycopg2', DatabaseConnectionConfig(
        type=DatabaseType.POSTGRESQL, user='postgres', password='postgres', database='understudy_test', host='127.0.0.1', port=5433)),
    'oracle': ('oracledb', DatabaseConnectionConfig(
        type=DatabaseType.ORACLE, user='system', password='oracle', database='understudy_test', host='127.0.0.1', port=1522,
        serviceName='understudy_test')),
    'mssql': ('pymssql', DatabaseConnectionConfig(
        type=DatabaseType.MSSQL, user='sa', password='YourStr0ng!Passw0rd', database='master', host='127.0.0.1', port=1434)),
    }

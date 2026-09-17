"""A runnable demonstration of masking, discovery and subsetting.

    python example/masking_demo.py

Needs no server and no credentials. It builds two throwaway SQLite databases,
one standing in for production and one for staging, loads its jobs from
example/configuration/masking/ the way the CLI does, and then:

1. Masks customers and orders into staging. The masked tables still join,
   because both key columns share a domain.
2. Adds a column to production that the policy doesn't cover, and runs again.
   The job fails before writing anything: new columns never leak by default.
3. Proposes a policy for the changed table, as `understudy discover` does.
4. Plans a referentially complete subset, as `understudy subset` does.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict

import yaml

exampleDirectory = Path(__file__).resolve().parent
sys.path.append(str(exampleDirectory.parent))

from understudy_data import (Configuration, Database, DataJobsFile, FileMemory, expandEnvironmentVariables, planSubset, proposeTable,
                             runDataJobs)

DEFAULT_WORKING_DIRECTORY = exampleDirectory / 'memory' / 'masking_demo'

DEMO_CONFIGURATION_DIRECTORY = exampleDirectory / 'configuration' / 'masking'

# A throwaway key for a throwaway database. A real key is random, lives in a
# secret store, and is never written into a script or a YAML file.
DEMO_MASKING_KEY = 'masking-demo-key-not-for-real-use'

SCHEMA = '''
CREATE TABLE customers (id INT PRIMARY KEY, email TEXT, full_name TEXT, phone TEXT, birth_date TEXT, region TEXT, notes TEXT);
CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT REFERENCES customers(id), amount NUMERIC);
'''

CUSTOMERS = [
    (1, 'ana.silva@corp.example', 'Ana Silva', '+351 912 345 678', '1985-04-12', 'eu', 'prefers email; allergic to penicillin'),
    (2, 'ben.okafor@corp.example', 'Ben Okafor', '+1 (555) 010-2233', '1990-11-02', 'us', None),
    (3, 'chen.li@corp.example', 'Chen Li', '+86 138 0013 8000', '1978-07-30', 'apac', 'VIP'),
    (4, 'dara.nolan@corp.example', 'Dara Nolan', '+353 87 123 4567', '2001-01-19', 'eu', None),
    (5, 'eve.martin@corp.example', 'Eve Martin', '+33 6 12 34 56 78', '1969-09-09', 'eu', 'called about a refund'),
    ]

ORDERS = [(100, 1, 25.00), (101, 1, 99.90), (102, 2, 12.50), (103, 3, 310.00), (104, 4, 45.25), (105, 5, 8.75), (106, 5, 60.00)]


def loadConfiguration(name: str) -> Any:
    """Loads a demo YAML file the same way the CLI does, ${NAME} expansion included."""

    with open(DEMO_CONFIGURATION_DIRECTORY / name) as file:
        return expandEnvironmentVariables(yaml.safe_load(file))


def printRows(heading: str, rows: Any) -> None:

    print('  {}'.format(heading))
    for row in rows:
        print('    {}'.format(row))


def main(workingDirectory: Path = DEFAULT_WORKING_DIRECTORY) -> Dict[str, Any]:
    """Runs the demonstration and returns what it observed, so the test that
    keeps this script from rotting can assert on it.
    """

    shutil.rmtree(workingDirectory, ignore_errors=True)
    workingDirectory.mkdir(parents=True, exist_ok=True)
    logPath = workingDirectory / 'masking.log'

    os.environ['MASKING_DEMO_PROD_PATH'] = str(workingDirectory / 'prod.db')
    os.environ['MASKING_DEMO_STAGING_PATH'] = str(workingDirectory / 'staging.db')
    os.environ.setdefault('MASKING_KEY', DEMO_MASKING_KEY)

    databases = Configuration.validateDatabaseConfiguration(loadConfiguration('database.yaml'))
    jobsFile = Configuration.validateJobConfiguration(loadConfiguration('jobs.yaml'), DataJobsFile)
    Configuration.validateJobGraph(jobsFile.jobs, databaseAliases=set(databases))
    memory = FileMemory(memoryFile=workingDirectory / 'memory.yaml')
    observed: Dict[str, Any] = {}

    for alias in databases:
        with Database(connectionSettings=databases[alias]) as database:
            database.connection.executescript(SCHEMA)

    with Database(connectionSettings=databases['prod']) as prod, Database(connectionSettings=databases['staging']) as staging:

        prod.insert(table='customers', data=CUSTOMERS, chunkSize=100)
        prod.insert(table='orders', data=ORDERS, chunkSize=100)

        print('\n1. MASK production into staging')
        result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, memory=memory, logFile=logPath, logLevel=logging.DEBUG)
        observed['firstRun'] = result.succeeded
        printRows('production customers:', prod.query('SELECT * FROM customers ORDER BY id'))
        printRows('staging customers:', staging.query('SELECT * FROM customers ORDER BY id'))

        joined = staging.query('SELECT count(*) FROM orders o JOIN customers c ON c.id = o.customer_id')[0][0]
        observed['joinedOrders'] = joined
        print('  {} of {} masked orders still join to a masked customer'.format(joined, len(ORDERS)))

        manifest = result.maskingManifest(jobsFile.jobs)
        observed['manifest'] = manifest
        manifestPath = workingDirectory / 'manifest.json'
        manifestPath.write_text(json.dumps(manifest, indent=2, default=str) + '\n')
        print('  manifest (key fingerprint {}) written to {}'.format(manifest['jobs'][0]['keyFingerprint'], manifestPath))

        print('\n2. ADD a column to production that the policy does not cover')
        prod.alter('ALTER TABLE customers ADD COLUMN ssn TEXT')
        prod.alter("UPDATE customers SET ssn = '123-45-' || (6780 + id)")
        staging.alter('ALTER TABLE customers ADD COLUMN ssn TEXT')
        prod.alter("INSERT INTO customers VALUES (6, 'fay@corp.example', 'Fay Ito', '+81 90 1234 5678', '1995-05-05', 'apac', NULL, '987-65-4321')")

        result = runDataJobs(jobsFile=jobsFile, databaseConfiguration=databases, memory=memory, logFile=logPath, logLevel=logging.DEBUG)
        [failure] = [outcome for outcome in result.outcomes if outcome.job == 'maskCustomers']
        observed['secondRun'] = failure
        observed['stagingCustomersAfterFailure'] = staging.query('SELECT count(*) FROM customers')[0][0]
        print('  maskCustomers {}: {}'.format(failure.status.value, failure.error))
        print('  staging still has {} customer(s), and no ssn values: {}'.format(
            observed['stagingCustomersAfterFailure'], staging.query('SELECT count(ssn) FROM customers')[0][0] == 0))

        print('\n3. DISCOVER a policy for the changed table')
        proposal = proposeTable(prod, 'customers', sampleSize=100)
        observed['proposal'] = {suggestion.column: suggestion.policy for suggestion in proposal.columns}
        for suggestion in proposal.columns:
            print('  {:<11} {:<48} # {}'.format(suggestion.column, json.dumps(suggestion.policy), suggestion.reason))

        print("\n4. SUBSET: customers in region 'eu', and everything they need")
        plan = planSubset(prod.getForeignKeys(), root='customers', where="region = 'eu'",
                          materialize=prod.dialect.supportsMaterializedSelections())
        observed['subset'] = {table: len(prod.query(plan.queries[table])) for table in plan.tables}
        for table in plan.tables:
            print('  {:<10} {} row(s), loaded after: {}'.format(table, observed['subset'][table], ', '.join(plan.parents[table]) or '-'))

    print('\nPer-chunk detail was logged at DEBUG to {}'.format(logPath))

    return observed


if __name__ == '__main__':
    main()

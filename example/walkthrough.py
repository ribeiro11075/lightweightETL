"""The whole workflow, end to end, through the same commands you would type.

    python example/walkthrough.py

Needs no server and no credentials. It builds a small shop's "production"
database in SQLite -- customers with national ids and phone numbers, orders,
support tickets full of personal details, and payment cards -- and then makes
a safe staging copy of its Portuguese and Spanish customers:

1. discover    proposes a masking policy for customers
2. subset      generates jobs for those customers and everything they need,
               with a proposed policy for every table; the script then
               applies the review a person would make
3. schema      creates staging's tables from production's
4. audit       checks the reviewed policy against production, and fails on
               anything a reviewer should look at
5. run         copies and masks the subset, writing a signed manifest and
               run history
6. verify-manifest   checks the manifest wasn't altered
7. synthesize  fills staging's payment cards with generated rows: card
               numbers never leave production, not even masked
8. history     shows what ran

Each command's output is printed as it runs, and the whole session is
written to walkthrough.md next to the databases.
"""
from __future__ import annotations

import contextlib
import io
import os
import random
import shutil
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import yaml

exampleDirectory = Path(__file__).resolve().parent
sys.path.append(str(exampleDirectory.parent))

from understudy_data.cli import main as cli  # noqa: E402

DEFAULT_WORKING_DIRECTORY = exampleDirectory / 'memory' / 'walkthrough'

# Throwaway keys for throwaway databases. Real ones are random, come from a
# secret store, and are never written into a script.
DEMO_MASKING_KEY = 'walkthrough-masking-key-not-for-real-use'
DEMO_SIGNING_KEY = 'walkthrough-signing-key-not-for-real-use'

SCHEMA = '''
CREATE TABLE customers (id INTEGER PRIMARY KEY, email VARCHAR(80) NOT NULL, full_name VARCHAR(60), national_id VARCHAR(11),
                        phone VARCHAR(20), birth_date DATE, country CHAR(2), signed_up DATE);
CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(40), price DECIMAL(8,2));
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), ordered_at TIMESTAMP, status VARCHAR(10));
CREATE TABLE order_items (order_id INT NOT NULL REFERENCES orders(id), line INT NOT NULL, product_id INT NOT NULL REFERENCES products(id),
                          quantity INT, PRIMARY KEY (order_id, line));
CREATE TABLE support_tickets (id INTEGER PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), opened_at TIMESTAMP, body TEXT);
CREATE TABLE payment_cards (id INTEGER PRIMARY KEY, customer_id INT NOT NULL REFERENCES customers(id), card_number VARCHAR(19), expiry CHAR(5));
'''

FIRST = ['Ana', 'Bruno', 'Carla', 'Diogo', 'Elena', 'Fabio', 'Gloria', 'Hugo', 'Ines', 'Jorge', 'Lucia', 'Marco', 'Nuria', 'Pablo']
LAST = ['Silva', 'Garcia', 'Santos', 'Lopez', 'Costa', 'Martin', 'Pereira', 'Ruiz', 'Almeida', 'Moreno']
COUNTRIES = ['PT', 'ES', 'FR', 'DE', 'US']
PRODUCTS = [(1, 'Espresso machine', '249.00'), (2, 'Grinder', '89.50'), (3, 'Beans 1kg', '18.90'), (4, 'Milk jug', '12.00'),
            (5, 'Descaler', '9.99')]
COMPLAINTS = [
    'Hi, this is {name}. My order never arrived; call me on {phone} or write to {email}.',
    'Card {card} was charged twice. Please refund. {name}',
    'Please update my delivery address. Reach me at {email}.',
    'The grinder is noisy. Thanks, {name}',
    ]


def buildProduction(path: Path) -> None:
    """A deterministic little shop, with personal data where real shops keep it."""

    rng = random.Random(7)
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA)
    customers, orders, items, tickets, cards = [], [], [], [], []

    for customerId in range(1, 61):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        name = '{} {}'.format(first, last)
        email = '{}.{}{}@mail.example'.format(first.lower(), last.lower(), customerId)
        phone = '+351 9{:02d} {:03d} {:03d}'.format(rng.randrange(100), rng.randrange(1000), rng.randrange(1000))
        card = '4{:03d} {:04d} {:04d} {:04d}'.format(rng.randrange(1000), rng.randrange(10000), rng.randrange(10000), rng.randrange(10000))
        customers.append((customerId, email, name, '{:09d}'.format(rng.randrange(10 ** 9)), phone,
                          (date(1950, 1, 1) + timedelta(days=rng.randrange(20000))).isoformat(), COUNTRIES[customerId % 5],
                          (date(2022, 1, 1) + timedelta(days=rng.randrange(1000))).isoformat()))
        cards.append((customerId, customerId, card, '{:02d}/{:02d}'.format(rng.randrange(1, 13), rng.randrange(27, 32))))
        for _ in range(rng.randrange(1, 4)):
            orderId = len(orders) + 1
            orders.append((orderId, customerId, (datetime(2025, 1, 1) + timedelta(minutes=rng.randrange(500000))).isoformat(sep=' '),
                           rng.choice(['paid', 'shipped', 'refunded'])))
            for line in range(1, rng.randrange(2, 4)):
                items.append((orderId, line, rng.choice(PRODUCTS)[0], rng.randrange(1, 4)))
        if rng.random() < 0.5:
            tickets.append((len(tickets) + 1, customerId, (datetime(2025, 6, 1) + timedelta(hours=rng.randrange(4000))).isoformat(sep=' '),
                            rng.choice(COMPLAINTS).format(name=name, phone=phone, email=email, card=card)))

    for table, rows in (('customers', customers), ('products', PRODUCTS), ('orders', orders), ('order_items', items),
                        ('support_tickets', tickets), ('payment_cards', cards)):
        connection.executemany('INSERT INTO {} VALUES ({})'.format(table, ', '.join('?' * len(rows[0]))), rows)
    connection.commit()
    connection.close()


def review(generated: Path, reviewed: Path) -> List[str]:
    """What a person reviewing the generated jobs decides, applied to the file.

    Returns the decisions, for the record.
    """

    document = yaml.safe_load(generated.read_text())
    jobs = document['jobs']
    decisions = []

    del jobs['maskPayment_cards']
    decisions.append('payment_cards: no copy job. Card numbers never leave production, not even masked; staging gets synthetic rows.')

    for job, columns in (('maskCustomers', ['id']), ('maskOrders', ['customer_id']), ('maskSupport_tickets', ['customer_id'])):
        for column in columns:
            jobs[job]['masking']['columns'][column] = {'strategy': 'key', 'domain': 'customer'}
    for job, column in (('maskOrders', 'id'), ('maskOrder_items', 'order_id')):
        jobs[job]['masking']['columns'][column] = {'strategy': 'key', 'domain': 'order'}
    decisions.append('customer and order ids: key, one domain each, so ids are masked and every join still holds.')

    customers = jobs['maskCustomers']['masking']['columns']
    customers['signed_up'] = {'strategy': 'dateShift', 'maxDays': 15}
    decisions.append('customers.signed_up: dateShift. A sign-up date plus a country can single someone out.')
    customers['national_id'] = {'strategy': 'fpe', 'charset': 'digits', 'strict': True}
    decisions.append('customers.national_id: fpe (NIST FF1), strict, as the data-protection policy requires a published algorithm.')
    customers['country'] = {'strategy': 'keep'}
    decisions.append('customers.country: keep. Analysts need it, and two letters identify no one.')

    tickets = jobs['maskSupport_tickets']['masking']['columns']
    tickets['body'] = {'strategy': 'redact', 'replacement': 'mask'}
    decisions.append('support_tickets.body: redact rather than null. Support needs the wording; phone numbers, emails and card numbers go.')

    heading = '# Generated by `understudy subset --mask`, then reviewed:\n' + ''.join('#   {}\n'.format(decision) for decision in decisions)
    reviewed.write_text(heading + yaml.safe_dump(document, sort_keys=False, width=200))

    return decisions


def main(workingDirectory: Path = DEFAULT_WORKING_DIRECTORY) -> Dict[str, Any]:
    """Runs the walkthrough, and returns what it observed so a test can check it."""

    shutil.rmtree(workingDirectory, ignore_errors=True)
    configuration = workingDirectory / 'configuration'
    configuration.mkdir(parents=True)
    production, staging = workingDirectory / 'production.db', workingDirectory / 'staging.db'
    buildProduction(production)
    sqlite3.connect(staging).close()
    (configuration / 'database.yaml').write_text(
        'production:\n  type: sqlite\n  database: {}\nstaging:\n  type: sqlite\n  database: {}\n'.format(production, staging))

    os.environ.setdefault('MASKING_KEY', DEMO_MASKING_KEY)
    os.environ.setdefault('UNDERSTUDY_MANIFEST_KEY', DEMO_SIGNING_KEY)

    # Commands run from the working directory, with the relative paths a
    # person would type.
    previousDirectory = os.getcwd()
    os.chdir(workingDirectory)
    try:
        return _walkthrough(workingDirectory, production, staging)
    finally:
        os.chdir(previousDirectory)


def _walkthrough(workingDirectory: Path, production: Path, staging: Path) -> Dict[str, Any]:

    transcript: List[str] = []
    observed: Dict[str, Any] = {'exitCodes': {}}

    def say(text: str) -> None:
        print(text)
        transcript.append(text)

    def command(step: str, *arguments: str, show: int = 40) -> str:
        """Runs one CLI command the way a person would, and records it."""

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            exitCode = cli(list(arguments) + ['--quiet'])
        observed['exitCodes'][step] = exitCode
        text = output.getvalue().rstrip()
        lines = text.splitlines()
        shown = '\n'.join(lines[:show] + (['... ({} more lines)'.format(len(lines) - show)] if len(lines) > show else []))
        say('\n$ understudy {}\n{}\n(exit {})'.format(' '.join(_quoted(argument) for argument in arguments), shown, exitCode))
        return text

    config = ['--config', 'configuration']

    say('# Walkthrough: a safe staging copy of production\n')
    say('Production has {} customers. Staging gets the Portuguese and Spanish ones, masked, with everything they reference.'.format(
        _count(production, 'customers')))

    say('\n## 1. Propose a policy')
    command('discover', 'discover', *config, '--database', 'production', '--table', 'customers', '--target', 'staging')

    say('\n## 2. Generate the subset, then review it')
    command('subset', 'subset', *config, '--database', 'production', '--target', 'staging', '--root', 'customers',
            '--where', "country IN ('PT', 'ES')", '--mask', '--output', 'configuration/generated.yaml')
    decisions = review(Path('configuration/generated.yaml'), Path('configuration/jobs.yaml'))
    say('Review decisions, recorded at the top of jobs.yaml:\n' + '\n'.join('- ' + decision for decision in decisions))

    say('\n## 3. Create staging\'s tables')
    command('schema', 'schema', *config, '--database', 'production', '--target', 'staging', '--table', 'customers', '--related', '--apply')

    say('\n## 4. Audit the policy against production')
    command('audit', 'audit', *config, '--connect', '--strict', show=60)

    say('\n## 5. Copy and mask')
    command('run', 'run', *config, '--manifest', 'manifest.json', '--history', 'history.jsonl')
    say('(Logs go to stderr, silenced here; the history below shows what ran.)')

    say('\n## 6. Check the manifest')
    command('verify-manifest', 'verify-manifest', 'manifest.json')

    say('\n## 7. Generate what may not be copied')
    command('synthesize', 'synthesize', *config, '--database', 'staging', '--table', 'payment_cards:40', '--seed', '1', '--yes')

    say('\n## 8. What ran')
    command('history', 'history', '--history', 'history.jsonl')

    say('\n## Before and after')
    observed.update(_compare(production, staging, say))

    report = workingDirectory / 'walkthrough.md'
    say('\nThis session was written to {}'.format(report))
    report.write_text('\n'.join(transcript) + '\n')

    return observed


def _quoted(argument: str) -> str:

    return '"{}"'.format(argument) if any(character in argument for character in " '()") else argument


def _count(path: Path, table: str, where: str = '1 = 1') -> int:

    connection = sqlite3.connect(path)
    try:
        return int(connection.execute('SELECT count(*) FROM {} WHERE {}'.format(table, where)).fetchone()[0])
    finally:
        connection.close()


def _compare(production: Path, staging: Path, say: Any) -> Dict[str, Any]:

    prod, stage = sqlite3.connect(production), sqlite3.connect(staging)
    try:
        say('Production customers (id, email, name, national id, phone, birth date, country, signed up):')
        for row in prod.execute("SELECT * FROM customers WHERE country IN ('PT', 'ES') ORDER BY id LIMIT 3"):
            say('  {}'.format(row))
        say('Staging customers, the same people, masked (in no particular order):')
        for row in stage.execute('SELECT * FROM customers ORDER BY email LIMIT 3'):
            say('  {}'.format(row))

        ticket = prod.execute("SELECT t.body FROM support_tickets t JOIN customers c ON c.id = t.customer_id "
                              "WHERE c.country IN ('PT', 'ES') AND t.body LIKE '%call me%' ORDER BY t.id LIMIT 1").fetchone()[0]
        masked = stage.execute("SELECT body FROM support_tickets WHERE body LIKE '%call me%' ORDER BY body LIMIT 1").fetchone()[0]
        say('A support ticket, before:\n  {}\nand after:\n  {}'.format(ticket, masked))
        say('The phone number and address are gone, but the name is still there: `redact` finds identifiers with a fixed shape, '
            'not names, which is what the audit noted. Where names matter, a reviewer chooses `null` instead.')

        observed = {
            'subsetCustomers': stage.execute('SELECT count(*) FROM customers').fetchone()[0],
            'expectedCustomers': prod.execute("SELECT count(*) FROM customers WHERE country IN ('PT', 'ES')").fetchone()[0],
            'foreignKeyViolations': stage.execute('PRAGMA foreign_key_check').fetchall(),
            'ordersJoined': stage.execute('SELECT count(*) FROM orders o JOIN customers c ON c.id = o.customer_id').fetchone()[0],
            'orders': stage.execute('SELECT count(*) FROM orders').fetchone()[0],
            'syntheticCards': stage.execute('SELECT count(*) FROM payment_cards').fetchone()[0],
            'productionEmailsInStaging': stage.execute('SELECT count(*) FROM customers WHERE email LIKE ?', ('%@mail.example',)).fetchone()[0],
            'productionIds': {row[0] for row in prod.execute("SELECT id FROM customers WHERE country IN ('PT', 'ES')")},
            'stagingIds': {row[0] for row in stage.execute('SELECT id FROM customers')},
            'productionCards': {row[0] for row in prod.execute('SELECT card_number FROM payment_cards')},
            'stagingCards': {row[0] for row in stage.execute('SELECT card_number FROM payment_cards')},
            'stagingTickets': [row[0] for row in stage.execute('SELECT body FROM support_tickets')],
            'productionTickets': [row[0] for row in prod.execute('SELECT body FROM support_tickets')],
            'maskedTicket': masked,
        }
        say('Staging has {subsetCustomers} customers (production has {expectedCustomers} in PT and ES), {orders} orders, all of which '
            'join ({ordersJoined}), {syntheticCards} synthetic payment cards, and {violations} foreign-key violations.'.format(
                violations=len(observed['foreignKeyViolations']), **observed))
        return observed
    finally:
        prod.close()
        stage.close()


if __name__ == '__main__':
    main()

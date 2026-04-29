"""
E-Commerce Distributed System - Main Flask Application
Runs all microservices with database replication, 2PC simulation, and failover
"""

import os
import sys
import time
import threading
import subprocess
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime
import logging

# Add shared modules to path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SHARED_DIR = os.path.join(CURRENT_DIR, 'shared')
if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)

from config import (
    Config, DatabaseConnection, TokenManager, 
    require_auth, require_admin, log_request_response, json_response, logger
)

# Initialize Flask app
app = Flask(__name__)
app.config.from_object(Config)
CORS(app)

# ========================
# Global Database Connections
# ========================
user_db = None
order_db = None
inventory_db = None
mongo_db = None
db_status = {'primary': {}, 'replica': {}}
failover_state = {
    'user_db': {'simulated_primary_down': False, 'promoted': False, 'last_action': None, 'updated_at': None},
    'order_db': {'simulated_primary_down': False, 'promoted': False, 'last_action': None, 'updated_at': None},
    'inventory_db': {'simulated_primary_down': False, 'promoted': False, 'last_action': None, 'updated_at': None},
}
active_db_endpoints = {
    'user_db': 'primary',
    'order_db': 'primary',
    'inventory_db': 'primary',
    'mongodb': 'primary',
}
auto_failover_state = {
    'enabled': False,
    'interval_seconds': 5,
    'last_run': None,
    'last_event': None,
    'thread_alive': False,
}
monitor_events = []
MAX_MONITOR_EVENTS = 500
auto_failover_stop_event = threading.Event()
auto_failover_thread = None
state_lock = threading.Lock()
last_connection_check_at = 0.0
CONNECTION_CHECK_INTERVAL_SECONDS = 2
REPO_ROOT = os.path.dirname(CURRENT_DIR)
COMPOSE_FILE = os.path.join(REPO_ROOT, 'docker-compose.yml')

NODE_CONTAINER_MAP = {
    ('user_db', 'primary'): 'user-db-primary',
    ('user_db', 'replica'): 'user-db-replica',
    ('order_db', 'primary'): 'order-db-primary',
    ('order_db', 'replica'): 'order-db-replica',
    ('inventory_db', 'primary'): 'inventory-db-primary',
    ('inventory_db', 'replica'): 'inventory-db-replica',
    ('mongodb', 'primary'): 'mongodb-primary',
    ('mongodb', 'secondary'): 'mongodb-secondary',
}


def current_timestamp_iso():
    return datetime.now().astimezone().isoformat()


def add_monitor_event(action, service=None, node=None, details=None, level='info'):
    event = {
        'timestamp': current_timestamp_iso(),
        'action': action,
        'service': service,
        'node': node,
        'level': level,
        'details': details or {},
    }
    with state_lock:
        monitor_events.append(event)
        if len(monitor_events) > MAX_MONITOR_EVENTS:
            del monitor_events[:len(monitor_events) - MAX_MONITOR_EVENTS]
    return event


def run_command(command, timeout=180):
    try:
        result = subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return result.returncode == 0, (result.stdout or '').strip(), (result.stderr or '').strip()
    except Exception as e:
        return False, '', str(e)


def run_docker_command(args, timeout=180):
    return run_command(['docker'] + args, timeout=timeout)


def run_compose_command(args, timeout=240):
    return run_command(['docker', 'compose', '-f', COMPOSE_FILE] + args, timeout=timeout)


def get_container_name(service, node):
    return NODE_CONTAINER_MAP.get((service, node))


def fail_node_actual(service, node):
    container = get_container_name(service, node)
    if not container:
        return False, f'Invalid service/node: {service}/{node}'
    ok, out, err = run_docker_command(['stop', container], timeout=120)
    if not ok:
        return False, err or out or 'Failed to stop container'
    add_monitor_event('node_failed', service=service, node=node, details={'container': container})
    return True, out or f'Stopped {container}'


def start_node_actual(service, node):
    container = get_container_name(service, node)
    if not container:
        return False, f'Invalid service/node: {service}/{node}'
    ok, out, err = run_docker_command(['start', container], timeout=180)
    if not ok:
        return False, err or out or 'Failed to start container'
    add_monitor_event('node_started', service=service, node=node, details={'container': container})
    return True, out or f'Started {container}'


def mongo_repl_command(host, port, command_doc):
    from pymongo import MongoClient

    connection_string = f"mongodb://{Config.MONGO_USER}:{Config.MONGO_PASSWORD}@{host}:{port}/admin?authSource=admin"
    client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
    return client.admin.command(command_doc)


def execute_mongo_promote_secondary():
    try:
        primary_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
        secondary_probe = probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT)

        if secondary_probe['status'] != 'healthy':
            return False, 'Mongo secondary is not healthy', secondary_probe

        # If setup is not a replica set (both standalone), treat promotion as active endpoint switch.
        if not secondary_probe.get('is_replicaset'):
            if secondary_probe['status'] != 'healthy':
                return False, 'Mongo secondary endpoint is not reachable', secondary_probe
            sync_mongo_standalone_data(['products', 'categories', 'reviews'])
            reconnect_mongo_db('secondary')
            with state_lock:
                active_db_endpoints['mongodb'] = 'secondary'
            add_monitor_event('mongo_active_switch', service='mongodb', node='secondary', details={'mode': 'standalone'})
            return True, 'Mongo active endpoint switched to secondary (standalone mode)', secondary_probe

        if secondary_probe.get('role') == 'primary':
            with state_lock:
                active_db_endpoints['mongodb'] = 'secondary'
            add_monitor_event('mongo_promote', service='mongodb', node='secondary', details={'already_primary': True})
            return True, 'Mongo secondary already primary', secondary_probe

        if primary_probe['status'] == 'healthy' and primary_probe.get('role') == 'primary':
            try:
                mongo_repl_command(Config.MONGO_HOST, Config.MONGO_PORT, {'replSetStepDown': 30, 'force': True})
            except Exception:
                pass

        mongo_repl_command(Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT, {'replSetStepUp': 1})

        new_secondary_probe = None
        for _ in range(12):
            time.sleep(1)
            new_secondary_probe = probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT)
            if new_secondary_probe['status'] == 'healthy' and new_secondary_probe.get('role') == 'primary':
                with state_lock:
                    active_db_endpoints['mongodb'] = 'secondary'
                add_monitor_event('mongo_promote', service='mongodb', node='secondary', details={'result': 'promoted'})
                return True, 'Mongo secondary promoted to primary', new_secondary_probe

        return False, 'Mongo promotion command sent but secondary did not become primary in time', new_secondary_probe
    except Exception as e:
        return False, str(e), None


def execute_mongo_promote_primary():
    """Promote mongo primary node back to writable primary when available."""
    try:
        primary_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
        secondary_probe = probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT)

        if primary_probe['status'] != 'healthy':
            return False, 'Mongo primary node is not healthy', primary_probe

        if not primary_probe.get('is_replicaset'):
            if primary_probe['status'] != 'healthy':
                return False, 'Mongo primary endpoint is not reachable', primary_probe
            reconnect_mongo_db('primary')
            with state_lock:
                active_db_endpoints['mongodb'] = 'primary'
            add_monitor_event('mongo_active_switch', service='mongodb', node='primary', details={'mode': 'standalone'})
            return True, 'Mongo active endpoint switched back to primary (standalone mode)', primary_probe

        if primary_probe.get('role') == 'primary':
            with state_lock:
                active_db_endpoints['mongodb'] = 'primary'
            add_monitor_event('mongo_promote', service='mongodb', node='primary', details={'already_primary': True})
            return True, 'Mongo primary is already writable primary', primary_probe

        if secondary_probe['status'] == 'healthy' and secondary_probe.get('role') == 'primary':
            try:
                mongo_repl_command(Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT, {'replSetStepDown': 30, 'force': True})
            except Exception:
                pass

        mongo_repl_command(Config.MONGO_HOST, Config.MONGO_PORT, {'replSetStepUp': 1})

        latest_primary = None
        for _ in range(12):
            time.sleep(1)
            latest_primary = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
            if latest_primary['status'] == 'healthy' and latest_primary.get('role') == 'primary':
                with state_lock:
                    active_db_endpoints['mongodb'] = 'primary'
                add_monitor_event('mongo_promote', service='mongodb', node='primary', details={'result': 'promoted'})
                return True, 'Mongo primary promoted back successfully', latest_primary

        return False, 'Mongo primary promotion command sent but node did not become primary in time', latest_primary
    except Exception as e:
        return False, str(e), None


def reset_all_databases_actual():
    stop_auto_failover()
    ok_down, out_down, err_down = run_compose_command(['down', '-v'], timeout=300)
    if not ok_down:
        return False, {'step': 'down', 'stdout': out_down, 'stderr': err_down}

    services = [
        'user-db-primary', 'user-db-replica',
        'order-db-primary', 'order-db-replica',
        'inventory-db-primary', 'inventory-db-replica',
        'mongodb-primary', 'mongodb-secondary',
    ]
    ok_up, out_up, err_up = run_compose_command(['up', '-d'] + services, timeout=360)
    if not ok_up:
        return False, {'step': 'up', 'stdout': out_up, 'stderr': err_up}

    with state_lock:
        failover_state['user_db'] = {'simulated_primary_down': False, 'promoted': False, 'last_action': 'reset', 'updated_at': current_timestamp_iso()}
        failover_state['order_db'] = {'simulated_primary_down': False, 'promoted': False, 'last_action': 'reset', 'updated_at': current_timestamp_iso()}
        failover_state['inventory_db'] = {'simulated_primary_down': False, 'promoted': False, 'last_action': 'reset', 'updated_at': current_timestamp_iso()}
        active_db_endpoints['user_db'] = 'primary'
        active_db_endpoints['order_db'] = 'primary'
        active_db_endpoints['inventory_db'] = 'primary'
        active_db_endpoints['mongodb'] = 'primary'

    return True, {'down': out_down, 'up': out_up}


def service_config(service):
    mapping = {
        'user_db': {
            'db_name': Config.USER_DB_NAME,
            'user': Config.USER_DB_USER,
            'password': Config.USER_DB_PASSWORD,
            'primary': (Config.USER_DB_HOST, Config.USER_DB_PORT),
            'replica': (Config.USER_DB_REPLICA_HOST, Config.USER_DB_REPLICA_PORT),
        },
        'order_db': {
            'db_name': Config.ORDER_DB_NAME,
            'user': Config.ORDER_DB_USER,
            'password': Config.ORDER_DB_PASSWORD,
            'primary': (Config.ORDER_DB_HOST, Config.ORDER_DB_PORT),
            'replica': (Config.ORDER_DB_REPLICA_HOST, Config.ORDER_DB_REPLICA_PORT),
        },
        'inventory_db': {
            'db_name': Config.INVENTORY_DB_NAME,
            'user': Config.INVENTORY_DB_USER,
            'password': Config.INVENTORY_DB_PASSWORD,
            'primary': (Config.INVENTORY_DB_HOST, Config.INVENTORY_DB_PORT),
            'replica': (Config.INVENTORY_DB_REPLICA_HOST, Config.INVENTORY_DB_REPLICA_PORT),
        },
    }
    return mapping.get(service)


def get_service_connection(service):
    if service == 'user_db':
        return user_db
    if service == 'order_db':
        return order_db
    if service == 'inventory_db':
        return inventory_db
    return None


def set_service_connection(service, conn):
    global user_db, order_db, inventory_db
    if service == 'user_db':
        user_db = conn
    elif service == 'order_db':
        order_db = conn
    elif service == 'inventory_db':
        inventory_db = conn


def reconnect_service_db(service, target_role):
    cfg = service_config(service)
    if not cfg:
        raise ValueError(f'Invalid service: {service}')

    host, port = cfg[target_role]
    new_conn = DatabaseConnection.get_postgres_connection(
        host,
        port,
        cfg['db_name'],
        cfg['user'],
        cfg['password'],
        timeout=5,
    )

    old_conn = get_service_connection(service)
    set_service_connection(service, new_conn)
    try:
        if old_conn:
            old_conn.close()
    except Exception:
        pass

    with state_lock:
        active_db_endpoints[service] = target_role


def ensure_service_connection(service):
    """Return a live service connection by trying active endpoint, then standby."""
    conn = get_service_connection(service)
    if is_postgres_connection_alive(conn):
        return conn

    cfg = service_config(service)
    if not cfg:
        raise RuntimeError(f'Invalid service: {service}')

    preferred_role = active_db_endpoints.get(service, 'primary')
    fallback_role = 'replica' if preferred_role == 'primary' else 'primary'

    last_error = None
    for role in (preferred_role, fallback_role):
        try:
            reconnect_service_db(service, role)
            reconnected = get_service_connection(service)
            if is_postgres_connection_alive(reconnected):
                return reconnected
        except Exception as e:
            last_error = e

    if last_error:
        raise RuntimeError(f'{service} unavailable: {str(last_error)}')
    raise RuntimeError(f'{service} unavailable')


def lsn_to_int(lsn_value):
    """Convert PostgreSQL WAL LSN text (e.g. '0/16B6C50') to an integer."""
    if not lsn_value or not isinstance(lsn_value, str) or '/' not in lsn_value:
        return None
    upper, lower = lsn_value.split('/', 1)
    try:
        return (int(upper, 16) << 32) + int(lower, 16)
    except Exception:
        return None


def wait_for_pg_replica_catchup(service, timeout_seconds=20, poll_interval=0.5):
    """Wait until replica replay LSN catches up to primary current WAL LSN.

    Returns (ok, message, details).
    """
    cfg = service_config(service)
    if not cfg:
        return False, f'Invalid service: {service}', None

    primary_conn = None
    primary_cursor = None
    replica_conn = None
    replica_cursor = None

    try:
        # If primary is unreachable, this is likely an emergency failover path.
        # In that case we allow forced promotion to continue.
        try:
            primary_conn = DatabaseConnection.get_postgres_connection(
                cfg['primary'][0],
                cfg['primary'][1],
                cfg['db_name'],
                cfg['user'],
                cfg['password'],
                timeout=3,
                max_retries=1,
                retry_delay=0,
                log_retries=False,
            )
            primary_cursor = primary_conn.cursor()
        except Exception as e:
            details = {'primary_reachable': False, 'reason': str(e)}
            return True, 'Primary unreachable; proceeding with forced promotion', details

        replica_conn = DatabaseConnection.get_postgres_connection(
            cfg['replica'][0],
            cfg['replica'][1],
            cfg['db_name'],
            cfg['user'],
            cfg['password'],
            timeout=3,
            max_retries=1,
            retry_delay=0,
            log_retries=False,
        )
        replica_cursor = replica_conn.cursor()

        start = time.time()
        latest_details = {'primary_reachable': True}

        while time.time() - start <= timeout_seconds:
            primary_cursor.execute('SELECT pg_current_wal_lsn()')
            primary_lsn = primary_cursor.fetchone()[0]

            replica_cursor.execute('SELECT pg_last_wal_replay_lsn(), pg_is_in_recovery()')
            replica_lsn, replica_in_recovery = replica_cursor.fetchone()

            primary_lsn_int = lsn_to_int(str(primary_lsn) if primary_lsn is not None else None)
            replica_lsn_int = lsn_to_int(str(replica_lsn) if replica_lsn is not None else None)

            lag_bytes = None
            if primary_lsn_int is not None and replica_lsn_int is not None:
                lag_bytes = max(primary_lsn_int - replica_lsn_int, 0)

            latest_details = {
                'primary_reachable': True,
                'primary_lsn': str(primary_lsn) if primary_lsn is not None else None,
                'replica_replay_lsn': str(replica_lsn) if replica_lsn is not None else None,
                'lag_bytes': lag_bytes,
                'replica_in_recovery': bool(replica_in_recovery),
            }

            if replica_in_recovery and lag_bytes == 0:
                return True, 'Replica caught up with primary WAL', latest_details

            time.sleep(poll_interval)

        return False, 'Replica has not caught up with primary; promotion blocked to prevent data loss', latest_details
    except Exception as e:
        return False, str(e), None
    finally:
        if primary_cursor:
            try:
                primary_cursor.close()
            except Exception:
                pass
        if primary_conn:
            try:
                primary_conn.close()
            except Exception:
                pass
        if replica_cursor:
            try:
                replica_cursor.close()
            except Exception:
                pass
        if replica_conn:
            try:
                replica_conn.close()
            except Exception:
                pass


def execute_pg_promote(service):
    cfg = service_config(service)
    if not cfg:
        return False, f'Invalid service: {service}', None

    host, port = cfg['replica']
    conn = None
    cursor = None
    try:
        conn = DatabaseConnection.get_postgres_connection(
            host,
            port,
            cfg['db_name'],
            cfg['user'],
            cfg['password'],
            timeout=5,
        )
        cursor = conn.cursor()
        cursor.execute('SELECT pg_is_in_recovery()')
        is_in_recovery = cursor.fetchone()[0]
        if not is_in_recovery:
            add_monitor_event('postgres_promote', service=service, node='replica', details={'already_promoted': True})
            return True, 'Replica already promoted (not in recovery)', probe_postgres_node(service, 'replica', host, port, cfg['db_name'])

        catchup_ok, catchup_message, catchup_details = wait_for_pg_replica_catchup(service)
        if not catchup_ok:
            add_monitor_event(
                'postgres_promote_blocked',
                service=service,
                node='replica',
                details={'reason': catchup_message, 'catchup': catchup_details},
            )
            return False, catchup_message, catchup_details

        cursor.execute('SELECT pg_promote()')

        promoted = False
        latest_status = None
        for _ in range(20):
            time.sleep(1)
            latest_status = probe_postgres_node(service, 'replica', host, port, cfg['db_name'])
            if latest_status.get('status') == 'healthy' and latest_status.get('is_in_recovery') is False:
                promoted = True
                break

        if not promoted:
            return False, 'Replica promotion command executed but role did not switch in time', latest_status

        reconnect_service_db(service, 'replica')
        add_monitor_event('postgres_promote', service=service, node='replica', details={'result': 'promoted', 'catchup': catchup_details})
        return True, 'Replica promoted and backend write routing switched', latest_status
    except Exception as e:
        return False, str(e), None
    finally:
        if cursor:
            cursor.close()
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def run_auto_failover_cycle():
    for service in ('user_db', 'order_db', 'inventory_db'):
        try:
            cfg = service_config(service)
            if not cfg:
                continue

            with state_lock:
                active_role = active_db_endpoints.get(service, 'primary')

            active_host, active_port = cfg[active_role]
            standby_role = 'replica' if active_role == 'primary' else 'primary'
            standby_host, standby_port = cfg[standby_role]

            # Do not auto-failback PostgreSQL to the original primary.
            # After replica promotion, switching back without re-seeding can hide newer writes.
            if active_role != 'primary':
                main_probe = probe_postgres_node(service, 'primary', cfg['primary'][0], cfg['primary'][1], cfg['db_name'])
                if main_probe.get('status') == 'healthy' and main_probe.get('is_in_recovery') is False:
                    with state_lock:
                        failover_state[service]['last_action'] = 'primary_recovered_no_auto_failback'
                        failover_state[service]['updated_at'] = current_timestamp_iso()
                        auto_failover_state['last_event'] = {
                            'service': service,
                            'action': 'primary_recovered_no_auto_failback',
                            'timestamp': current_timestamp_iso(),
                            'message': 'Primary recovered; keeping promoted replica active to avoid stale data.',
                        }

            active_probe = probe_postgres_node(service, 'primary', active_host, active_port, cfg['db_name'])
            if active_probe.get('status') == 'healthy':
                continue

            standby_probe = probe_postgres_node(service, 'replica', standby_host, standby_port, cfg['db_name'])
            if standby_probe.get('status') != 'healthy':
                with state_lock:
                    failover_state[service]['last_action'] = 'auto_failover_blocked'
                    failover_state[service]['updated_at'] = current_timestamp_iso()
                    auto_failover_state['last_event'] = {
                        'service': service,
                        'action': 'auto_failover_blocked',
                        'timestamp': current_timestamp_iso(),
                        'reason': 'Both active and standby nodes unhealthy',
                    }
                continue

            if standby_probe.get('is_in_recovery'):
                ok, message, promoted_status = execute_pg_promote(service)
                with state_lock:
                    failover_state[service]['simulated_primary_down'] = False
                    failover_state[service]['promoted'] = bool(ok)
                    failover_state[service]['last_action'] = 'auto_promote' if ok else 'auto_promote_failed'
                    failover_state[service]['updated_at'] = current_timestamp_iso()
                    auto_failover_state['last_event'] = {
                        'service': service,
                        'action': 'auto_promote' if ok else 'auto_promote_failed',
                        'timestamp': current_timestamp_iso(),
                        'message': message,
                        'status': promoted_status,
                    }
            else:
                reconnect_service_db(service, standby_role)
                with state_lock:
                    failover_state[service]['simulated_primary_down'] = False
                    failover_state[service]['promoted'] = True
                    failover_state[service]['last_action'] = 'auto_route_switch'
                    failover_state[service]['updated_at'] = current_timestamp_iso()
                    auto_failover_state['last_event'] = {
                        'service': service,
                        'action': 'auto_route_switch',
                        'timestamp': current_timestamp_iso(),
                        'message': 'Standby already writable; switched backend routing',
                    }
        except Exception as service_error:
            logger.error(f"Auto failover service cycle error ({service}): {str(service_error)}")
            with state_lock:
                failover_state[service]['last_action'] = 'auto_cycle_error'
                failover_state[service]['updated_at'] = current_timestamp_iso()
                auto_failover_state['last_event'] = {
                    'service': service,
                    'action': 'auto_cycle_error',
                    'timestamp': current_timestamp_iso(),
                    'message': str(service_error),
                }

    # MongoDB auto failover/failback based on active endpoint and node health.
    with state_lock:
        mongo_active = active_db_endpoints.get('mongodb', 'primary')

    mongo_primary = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
    mongo_secondary = probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT)

    if mongo_active == 'primary' and mongo_primary.get('status') != 'healthy' and mongo_secondary.get('status') == 'healthy':
        ok, message, status = execute_mongo_promote_secondary()
        with state_lock:
            auto_failover_state['last_event'] = {
                'service': 'mongodb',
                'action': 'auto_promote' if ok else 'auto_promote_failed',
                'timestamp': current_timestamp_iso(),
                'message': message,
                'status': status,
            }

    if mongo_active == 'secondary':
        main_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
        if main_probe.get('status') == 'healthy':
            ok, message, status = execute_mongo_promote_primary()
            with state_lock:
                auto_failover_state['last_event'] = {
                    'service': 'mongodb',
                    'action': 'auto_failback_main' if ok else 'auto_failback_failed',
                    'timestamp': current_timestamp_iso(),
                    'message': message,
                    'status': status,
                }


def passive_failback_to_primary():
    """Best-effort failback on status polling when monitor is not running.

    PostgreSQL is intentionally excluded: automatic switchback can route traffic to
    an out-of-date primary after replica promotion.
    """

    with state_lock:
        mongo_active = active_db_endpoints.get('mongodb', 'primary')

    if mongo_active == 'secondary':
        main_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
        if main_probe.get('status') == 'healthy':
            try:
                reconnect_mongo_db('primary')
                with state_lock:
                    active_db_endpoints['mongodb'] = 'primary'
            except Exception as e:
                logger.warning(f"Passive failback failed for mongodb: {str(e)}")


def auto_failover_loop():
    with state_lock:
        auto_failover_state['thread_alive'] = True
    try:
        while not auto_failover_stop_event.wait(auto_failover_state['interval_seconds']):
            with state_lock:
                auto_failover_state['last_run'] = current_timestamp_iso()
            try:
                run_auto_failover_cycle()
            except Exception as e:
                logger.error(f'Auto failover cycle error: {str(e)}')
                with state_lock:
                    auto_failover_state['last_event'] = {
                        'action': 'cycle_error',
                        'timestamp': current_timestamp_iso(),
                        'message': str(e),
                    }
    finally:
        with state_lock:
            auto_failover_state['thread_alive'] = False
            if auto_failover_state['enabled'] and auto_failover_stop_event.is_set():
                auto_failover_state['enabled'] = False


def start_auto_failover(interval_seconds=5):
    global auto_failover_thread
    with state_lock:
        thread_is_running = bool(auto_failover_thread and auto_failover_thread.is_alive())
        if auto_failover_state['enabled'] and thread_is_running:
            return False

        if auto_failover_state['enabled'] and not thread_is_running:
            auto_failover_state['enabled'] = False

        auto_failover_state['enabled'] = True
        auto_failover_state['interval_seconds'] = max(2, int(interval_seconds))
        auto_failover_state['last_event'] = {
            'action': 'monitor_started',
            'timestamp': current_timestamp_iso(),
        }
    add_monitor_event('monitor_started', details={'interval_seconds': max(2, int(interval_seconds))})
    auto_failover_stop_event.clear()
    auto_failover_thread = threading.Thread(target=auto_failover_loop, daemon=True)
    auto_failover_thread.start()
    return True


def stop_auto_failover():
    with state_lock:
        if not auto_failover_state['enabled']:
            return False
        auto_failover_state['enabled'] = False
        auto_failover_state['last_event'] = {
            'action': 'monitor_stopped',
            'timestamp': current_timestamp_iso(),
        }
    add_monitor_event('monitor_stopped')
    auto_failover_stop_event.set()
    return True


def apply_failover_overrides(nodes):
    """Apply in-memory failover simulation state to probed nodes."""
    for node in nodes:
        service = node.get('service')
        if service not in failover_state:
            continue

        state = failover_state[service]

        if node.get('node') == 'primary' and state.get('simulated_primary_down'):
            node['status'] = 'unhealthy'
            node['role'] = 'primary (simulated failed)'
            node['error'] = 'Simulated outage triggered by admin'

        node['failover_state'] = {
            'simulated_primary_down': bool(state.get('simulated_primary_down')),
            'promoted': bool(state.get('promoted')),
            'last_action': state.get('last_action'),
            'updated_at': state.get('updated_at'),
        }

        node['replication_model'] = 'PostgreSQL physical streaming replication (async hot standby)'

    for node in nodes:
        if node.get('service') == 'mongodb':
            if node.get('is_replicaset'):
                node['replication_model'] = 'MongoDB replica set oplog replication (asynchronous)'
            else:
                node['replication_model'] = 'MongoDB standalone nodes (active endpoint failover without oplog replication)'

    return nodes

def safe_rollback(conn):
    """Rollback only when a connection exists."""
    if conn is None:
        return
    try:
        conn.rollback()
    except Exception as e:
        logger.warning(f"Rollback skipped: {str(e)}")

def reset_postgres_states():
    """Clear aborted transaction state for long-lived DB connections."""
    safe_rollback(user_db)
    safe_rollback(order_db)
    safe_rollback(inventory_db)


def is_postgres_connection_alive(conn):
    """Return True when the PostgreSQL connection can execute a trivial query."""
    if conn is None:
        return False

    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT 1')
        cursor.fetchone()
        return True
    except Exception:
        return False
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def is_mongo_connection_alive(db):
    """Return True when the active Mongo connection can respond to ping."""
    if db is None:
        return False

    try:
        db.client.admin.command('ping')
        return True
    except Exception:
        return False


def refresh_dead_connections(force=False):
    """Mark dead connections unavailable so they are reinitialized on this request."""
    global mongo_db, last_connection_check_at

    now = time.time()
    if not force and (now - last_connection_check_at) < CONNECTION_CHECK_INTERVAL_SECONDS:
        return
    last_connection_check_at = now

    for service in ('user_db', 'order_db', 'inventory_db'):
        conn = get_service_connection(service)
        if conn is not None and not is_postgres_connection_alive(conn):
            logger.warning(f"Detected stale {service} connection; scheduling reconnect")
            db_status['primary'][service] = 'disconnected'
            set_service_connection(service, None)
            try:
                conn.close()
            except Exception:
                pass

    if mongo_db is not None and not is_mongo_connection_alive(mongo_db):
        logger.warning('Detected stale mongodb connection; scheduling reconnect')
        db_status['primary']['mongodb'] = 'disconnected'
        try:
            mongo_db.client.close()
        except Exception:
            pass
        mongo_db = None

def databases_available():
    """Ensure all required DB connections are ready."""
    required = {
        'user_db': user_db,
        'order_db': order_db,
        'inventory_db': inventory_db,
        'mongodb': mongo_db,
    }
    return all(conn is not None for conn in required.values())

def call_db_routine(conn, cursor, routine_name, args=None):
    """Call SQL routine across drivers.

    psycopg2 supports cursor.callproc well with PostgreSQL functions.
    pg8000's callproc expects procedures, so run functions via SELECT.
    """
    args = args or []
    if getattr(conn, '_use_function_select', False):
        placeholders = ', '.join(['%s'] * len(args))
        sql = f"SELECT * FROM {routine_name}({placeholders})" if placeholders else f"SELECT * FROM {routine_name}()"
        try:
            cursor.execute(sql, args)
        except Exception:
            safe_rollback(conn)
            raise
        return
    try:
        cursor.callproc(routine_name, args)
    except Exception:
        safe_rollback(conn)
        raise

def probe_postgres_node(service, node, host, port, database):
    """Probe a PostgreSQL node and return detailed replication metadata."""
    start = time.time()
    conn = None
    cursor = None
    endpoint = f"{host}:{port}"
    try:
        conn = DatabaseConnection.get_postgres_connection(
            host,
            port,
            database,
            Config.USER_DB_USER,
            Config.USER_DB_PASSWORD,
            timeout=1,
            max_retries=1,
            retry_delay=0,
            log_retries=False,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT pg_is_in_recovery(), version()")
        is_replica, version = cursor.fetchone()

        lsn = None
        if is_replica:
            cursor.execute("SELECT pg_last_wal_replay_lsn()")
            lsn = cursor.fetchone()[0]
        else:
            cursor.execute("SELECT pg_current_wal_lsn()")
            lsn = cursor.fetchone()[0]

        latency_ms = int((time.time() - start) * 1000)
        return {
            'service': service,
            'node': node,
            'endpoint': endpoint,
            'database': database,
            'status': 'healthy',
            'role': 'replica' if is_replica else 'primary',
            'is_in_recovery': bool(is_replica),
            'wal_lsn': str(lsn) if lsn is not None else None,
            'latency_ms': latency_ms,
            'version': version.split(' on ')[0] if version else None,
            'checked_at': datetime.utcnow().isoformat(),
            'error': None,
        }
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        return {
            'service': service,
            'node': node,
            'endpoint': endpoint,
            'database': database,
            'status': 'unhealthy',
            'role': 'unknown',
            'is_in_recovery': None,
            'wal_lsn': None,
            'latency_ms': latency_ms,
            'version': None,
            'checked_at': datetime.utcnow().isoformat(),
            'error': str(e),
        }
    finally:
        if cursor:
            cursor.close()
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def probe_mongo_node(node, host, port):
    """Probe Mongo node and return primary/secondary role details."""
    start = time.time()
    endpoint = f"{host}:{port}"
    try:
        from pymongo import MongoClient
        connection_string = f"mongodb://{Config.MONGO_USER}:{Config.MONGO_PASSWORD}@{host}:{port}/admin?authSource=admin"
        client = MongoClient(connection_string, serverSelectionTimeoutMS=3000)
        hello = client.admin.command('hello')
        latency_ms = int((time.time() - start) * 1000)
        is_replset = bool(hello.get('setName'))
        role = 'primary' if hello.get('isWritablePrimary') else 'replica'
        if not is_replset:
            # Preserve UI semantics for primary/secondary rows in standalone mode.
            role = 'primary' if node == 'primary' else 'secondary'
        return {
            'service': 'mongodb',
            'node': node,
            'endpoint': endpoint,
            'database': Config.MONGO_DB,
            'status': 'healthy',
            'role': role,
            'is_in_recovery': not hello.get('isWritablePrimary', False),
            'is_replicaset': is_replset,
            'wal_lsn': None,
            'latency_ms': latency_ms,
            'version': hello.get('version'),
            'checked_at': datetime.utcnow().isoformat(),
            'error': None,
        }
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        return {
            'service': 'mongodb',
            'node': node,
            'endpoint': endpoint,
            'database': Config.MONGO_DB,
            'status': 'unhealthy',
            'role': 'unknown',
            'is_in_recovery': None,
            'is_replicaset': None,
            'wal_lsn': None,
            'latency_ms': latency_ms,
            'version': None,
            'checked_at': datetime.utcnow().isoformat(),
            'error': str(e),
        }


def reconnect_mongo_db(target_role):
    global mongo_db
    if target_role == 'secondary':
        host, port = Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT
    else:
        host, port = Config.MONGO_HOST, Config.MONGO_PORT

    mongo_db = DatabaseConnection.get_mongo_connection(
        host,
        port,
        Config.MONGO_DB,
        Config.MONGO_USER,
        Config.MONGO_PASSWORD,
    )


def get_mongo_db_for_role(target_role):
    """Create a short-lived Mongo database handle for a specific role."""
    if target_role == 'secondary':
        host, port = Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT
    else:
        host, port = Config.MONGO_HOST, Config.MONGO_PORT

    return DatabaseConnection.get_mongo_connection(
        host,
        port,
        Config.MONGO_DB,
        Config.MONGO_USER,
        Config.MONGO_PASSWORD,
    )


def sync_mongo_standalone_data(collection_names=None):
    """Mirror data from Mongo primary to secondary when running standalone nodes."""
    if collection_names is None:
        collection_names = ['products', 'categories', 'reviews']

    primary_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
    secondary_probe = probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT)

    if primary_probe.get('status') != 'healthy' or secondary_probe.get('status') != 'healthy':
        return False, 'Primary or secondary Mongo endpoint is unhealthy', {
            'primary': primary_probe,
            'secondary': secondary_probe,
        }

    if primary_probe.get('is_replicaset'):
        return True, 'Replica set mode detected; standalone sync not needed', None

    try:
        primary_db = get_mongo_db_for_role('primary')
        secondary_db = get_mongo_db_for_role('secondary')
        stats = {}

        for collection_name in collection_names:
            source_docs = list(primary_db[collection_name].find({}))
            source_ids = []

            for doc in source_docs:
                source_ids.append(doc['_id'])
                secondary_db[collection_name].replace_one({'_id': doc['_id']}, doc, upsert=True)

            if source_ids:
                secondary_db[collection_name].delete_many({'_id': {'$nin': source_ids}})
            else:
                secondary_db[collection_name].delete_many({})

            stats[collection_name] = len(source_docs)

        return True, 'Mongo standalone sync completed', stats
    except Exception as e:
        return False, str(e), None


def read_products_with_mongo_fallback(query, page, limit):
    """Read products with endpoint fallback for standalone Mongo deployments."""
    skip = (page - 1) * limit
    active_role = active_db_endpoints.get('mongodb', 'primary')
    alternate_role = 'secondary' if active_role == 'primary' else 'primary'

    errors = []

    # Try current active connection first.
    try:
        products = list(mongo_db.products.find(query).skip(skip).limit(limit))
        total = mongo_db.products.count_documents(query)
        if active_role == 'secondary' and total == 0:
            sync_ok, _, _ = sync_mongo_standalone_data(['products'])
            if sync_ok:
                products = list(mongo_db.products.find(query).skip(skip).limit(limit))
                total = mongo_db.products.count_documents(query)
        if total > 0 or active_role == 'primary':
            return products, total, active_role
    except Exception as e:
        errors.append(f"{active_role}: {str(e)}")

    # If active endpoint is empty or failed, check alternate endpoint.
    try:
        alt_db = get_mongo_db_for_role(alternate_role)
        alt_products = list(alt_db.products.find(query).skip(skip).limit(limit))
        alt_total = alt_db.products.count_documents(query)
        if alt_total > 0:
            return alt_products, alt_total, alternate_role
    except Exception as e:
        errors.append(f"{alternate_role}: {str(e)}")

    if errors:
        raise Exception('; '.join(errors))

    return [], 0, active_role

def get_user_is_admin(user_id):
    """Fetch admin flag from users table."""
    cursor = None
    try:
        cursor = user_db.cursor()
        cursor.execute('SELECT COALESCE(is_admin, FALSE) FROM users WHERE id = %s', (user_id,))
        row = cursor.fetchone()
        return bool(row and row[0])
    except Exception as e:
        logger.warning(f"Unable to fetch admin flag for user {user_id}: {str(e)}")
        return False
    finally:
        if cursor:
            cursor.close()

def ensure_default_admin_user():
    """Ensure requested admin account exists for admin panel access."""
    cursor = None
    try:
        cursor = user_db.cursor()
        cursor.execute(
            """
            INSERT INTO users (name, email, password, phone, is_admin, is_active)
            VALUES (%s, %s, %s, %s, TRUE, TRUE)
            ON CONFLICT (email)
            DO UPDATE SET
                name = EXCLUDED.name,
                password = EXCLUDED.password,
                is_admin = TRUE,
                is_active = TRUE,
                updated_at = CURRENT_TIMESTAMP
            """,
            ('System Admin', 'admin@distribuy.com', 'admin', '+1000000000'),
        )
        user_db.commit()
        logger.info('Default admin user ensured: admin@distribuy.com')
    except Exception as e:
        safe_rollback(user_db)
        logger.warning(f"Could not ensure default admin user: {str(e)}")
    finally:
        if cursor:
            cursor.close()


def ensure_user_audit_log_schema():
    """Allow delete audit rows to be persisted after user deletion.

    user_audit_log stores historical records and should not require live users row
    presence for DELETE actions.
    """
    cursor = None
    try:
        if user_db is None:
            return

        cursor = user_db.cursor()
        cursor.execute("ALTER TABLE user_audit_log DROP CONSTRAINT IF EXISTS user_audit_log_user_id_fkey")
        user_db.commit()
    except Exception as e:
        safe_rollback(user_db)
        logger.warning(f"Could not adjust user_audit_log schema: {str(e)}")
    finally:
        if cursor:
            cursor.close()

def initialize_databases():
    """Initialize all database connections"""
    global user_db, order_db, inventory_db, mongo_db, db_status

    connected_any = False

    def connect_service(service):
        cfg = service_config(service)
        if not cfg:
            return None, None

        preferred_role = active_db_endpoints.get(service, 'primary')
        fallback_role = 'replica' if preferred_role == 'primary' else 'primary'
        roles_to_try = [preferred_role, fallback_role]

        last_error = None
        for role in roles_to_try:
            host, port = cfg[role]
            try:
                conn = DatabaseConnection.get_postgres_connection(
                    host,
                    port,
                    cfg['db_name'],
                    cfg['user'],
                    cfg['password'],
                )
                with state_lock:
                    active_db_endpoints[service] = role
                return conn, role
            except Exception as e:
                last_error = e

        raise last_error if last_error else RuntimeError(f"No endpoint available for {service}")

    try:
        user_db, user_role = connect_service('user_db')
        db_status['primary']['user_db'] = 'connected'
        ensure_user_audit_log_schema()
        # Avoid write-on-login bootstrap on read-only standby.
        if user_role == 'primary':
            ensure_default_admin_user()
        connected_any = True
    except Exception as e:
        user_db = None
        db_status['primary']['user_db'] = 'disconnected'
        logger.warning(f"User DB initialization failed: {str(e)}")

    try:
        order_db, _ = connect_service('order_db')
        db_status['primary']['order_db'] = 'connected'
        connected_any = True
    except Exception as e:
        order_db = None
        db_status['primary']['order_db'] = 'disconnected'
        logger.warning(f"Order DB initialization failed: {str(e)}")

    try:
        inventory_db, _ = connect_service('inventory_db')
        db_status['primary']['inventory_db'] = 'connected'
        connected_any = True
    except Exception as e:
        inventory_db = None
        db_status['primary']['inventory_db'] = 'disconnected'
        logger.warning(f"Inventory DB initialization failed: {str(e)}")

    try:
        mongo_db = DatabaseConnection.get_mongo_connection(
            Config.MONGO_HOST, Config.MONGO_PORT,
            Config.MONGO_DB, Config.MONGO_USER, Config.MONGO_PASSWORD
        )
        db_status['primary']['mongodb'] = 'connected'
        connected_any = True
        sync_ok, sync_message, _ = sync_mongo_standalone_data(['products', 'categories', 'reviews'])
        if not sync_ok:
            logger.warning(f"Mongo standalone sync skipped: {sync_message}")
    except Exception as e:
        mongo_db = None
        db_status['primary']['mongodb'] = 'disconnected'
        logger.warning(f"MongoDB initialization failed: {str(e)}")

    if connected_any:
        logger.info("Database initialization completed with partial-availability support")
    else:
        logger.error("Database initialization failed: no database connections available")
    return connected_any

# Initialize on startup
@app.before_request
def init_db_on_first_request():
    """Initialize databases on first request"""
    if request.path == '/health':
        return None

    reset_postgres_states()
    refresh_dead_connections()

    if not databases_available():
        if not initialize_databases():
            logger.warning("Database initialization failed")
            allowed_during_outage = (
                request.path.startswith('/api/admin/')
                or request.path.startswith('/api/health/')
            )
            if not allowed_during_outage:
                return json_response(503, error='Database connections unavailable')

# ========================
# HEALTH CHECK ENDPOINTS
# ========================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return json_response(200, data={
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'database_status': db_status
    })

@app.route('/api/health/databases', methods=['GET'])
@require_auth
def database_health():
    """Get detailed database health status"""
    try:
        status = {}
        
        # Check primary databases
        try:
            safe_rollback(user_db)
            cursor = user_db.cursor()
            cursor.execute('SELECT 1')
            status['user_db_primary'] = 'healthy'
            cursor.close()
        except:
            status['user_db_primary'] = 'unhealthy'
        
        try:
            safe_rollback(order_db)
            cursor = order_db.cursor()
            cursor.execute('SELECT 1')
            status['order_db_primary'] = 'healthy'
            cursor.close()
        except:
            status['order_db_primary'] = 'unhealthy'
        
        try:
            safe_rollback(inventory_db)
            cursor = inventory_db.cursor()
            cursor.execute('SELECT 1')
            status['inventory_db_primary'] = 'healthy'
            cursor.close()
        except:
            status['inventory_db_primary'] = 'unhealthy'
        
        try:
            mongo_db.command('ping')
            status['mongodb_primary'] = 'healthy'
        except:
            status['mongodb_primary'] = 'unhealthy'

        status['user_db_replica'] = probe_postgres_node(
            'user_db', 'replica', Config.USER_DB_REPLICA_HOST, Config.USER_DB_REPLICA_PORT, Config.USER_DB_NAME
        )['status']
        status['order_db_replica'] = probe_postgres_node(
            'order_db', 'replica', Config.ORDER_DB_REPLICA_HOST, Config.ORDER_DB_REPLICA_PORT, Config.ORDER_DB_NAME
        )['status']
        status['inventory_db_replica'] = probe_postgres_node(
            'inventory_db', 'replica', Config.INVENTORY_DB_REPLICA_HOST, Config.INVENTORY_DB_REPLICA_PORT, Config.INVENTORY_DB_NAME
        )['status']
        status['mongodb_secondary'] = probe_mongo_node(
            'secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT
        )['status']
        
        return json_response(200, data=status)
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/health/replication', methods=['GET'])
@require_auth
@require_admin
def replication_health():
    """Detailed replication and node health diagnostics."""
    try:
        passive_failback_to_primary()

        nodes = [
            probe_postgres_node('user_db', 'primary', Config.USER_DB_HOST, Config.USER_DB_PORT, Config.USER_DB_NAME),
            probe_postgres_node('user_db', 'replica', Config.USER_DB_REPLICA_HOST, Config.USER_DB_REPLICA_PORT, Config.USER_DB_NAME),
            probe_postgres_node('order_db', 'primary', Config.ORDER_DB_HOST, Config.ORDER_DB_PORT, Config.ORDER_DB_NAME),
            probe_postgres_node('order_db', 'replica', Config.ORDER_DB_REPLICA_HOST, Config.ORDER_DB_REPLICA_PORT, Config.ORDER_DB_NAME),
            probe_postgres_node('inventory_db', 'primary', Config.INVENTORY_DB_HOST, Config.INVENTORY_DB_PORT, Config.INVENTORY_DB_NAME),
            probe_postgres_node('inventory_db', 'replica', Config.INVENTORY_DB_REPLICA_HOST, Config.INVENTORY_DB_REPLICA_PORT, Config.INVENTORY_DB_NAME),
            probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT),
            probe_mongo_node('secondary', Config.MONGO_SECONDARY_HOST, Config.MONGO_SECONDARY_PORT),
        ]

        nodes = apply_failover_overrides(nodes)
        healthy_count = len([n for n in nodes if n['status'] == 'healthy'])
        failover_active = any(
            s.get('simulated_primary_down') or s.get('promoted')
            for s in failover_state.values()
        )
        summary = {
            'healthy_nodes': healthy_count,
            'total_nodes': len(nodes),
            'overall_status': 'healthy' if healthy_count == len(nodes) and not failover_active else 'degraded',
            'failover_active': failover_active,
            'replication_types': {
                'postgresql': 'Physical streaming replication (asynchronous hot standby)',
                'mongodb': (
                    'Replica set oplog replication (asynchronous)'
                    if any(n.get('service') == 'mongodb' and n.get('is_replicaset') for n in nodes)
                    else 'Standalone endpoints with active-endpoint failover (no data replication)'
                )
            },
            'checked_at': current_timestamp_iso(),
        }

        return json_response(200, data={
            'summary': summary,
            'nodes': nodes,
            'failover_state': failover_state,
            'active_db_endpoints': active_db_endpoints,
            'auto_failover': auto_failover_state,
        })
    except Exception as e:
        logger.error(f"Replication health error: {str(e)}")
        return json_response(500, error=str(e))

# ========================
# USER SERVICE ENDPOINTS
# ========================

@app.route('/api/users/register', methods=['POST'])
def user_register():
    """Register a new user"""
    cursor = None
    conn = None
    try:
        data = request.get_json()
        
        # Validate input
        required_fields = ['name', 'email', 'password']
        if not all(field in data for field in required_fields):
            return json_response(400, error='Missing required fields')
        
        name = data.get('name')
        email = data.get('email')
        password = data.get('password')
        phone = data.get('phone')
        
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        
        # Call stored procedure
        call_db_routine(conn, cursor, 'register_user', [name, email, password, phone])
        result = cursor.fetchone()
        conn.commit()
        
        if result[0]:  # success
            is_admin = get_user_is_admin(result[1])
            token = TokenManager.generate_token(result[1], email, is_admin)
            return json_response(201, data={
                'user_id': result[1],
                'message': result[2],
                'is_admin': is_admin,
                'token': token
            })
        else:
            return json_response(400, error=result[2])
            
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Registration error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/users/login', methods=['POST'])
def user_login():
    """Authenticate user and generate token"""
    cursor = None
    conn = None
    try:
        data = request.get_json()
        
        if 'email' not in data or 'password' not in data:
            return json_response(400, error='Email and password required')
        
        email = data.get('email')
        password = data.get('password')
        
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        call_db_routine(conn, cursor, 'authenticate_user', [email, password])
        result = cursor.fetchone()
        
        if result[0]:  # success
            is_admin = get_user_is_admin(result[1])
            token = TokenManager.generate_token(result[1], email, is_admin)
            return json_response(200, data={
                'user_id': result[1],
                'is_admin': is_admin,
                'token': token,
                'message': result[2]
            })
        else:
            return json_response(401, error=result[2])
            
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Login error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/users/profile', methods=['GET'])
@require_auth
def get_user_profile():
    """Get user profile"""
    cursor = None
    conn = None
    try:
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id AS user_id,
                name,
                email,
                phone,
                address,
                city,
                state,
                zip_code,
                country,
                is_admin,
                is_active,
                created_at,
                last_login
            FROM users
            WHERE id = %s AND is_active = TRUE
            """,
            (request.user_id,),
        )
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        
        if result:
            profile = dict(zip(columns, result))
            return json_response(200, data=profile)
        else:
            return json_response(404, error='User not found')
            
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Error fetching profile: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/users/profile', methods=['PUT'])
@require_auth
def update_user_profile():
    """Update user profile"""
    cursor = None
    conn = None
    try:
        data = request.get_json()
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        
        call_db_routine(conn, cursor, 'update_user_profile', [
            request.user_id,
            data.get('name'),
            data.get('phone'),
            data.get('address'),
            data.get('city'),
            data.get('state'),
            data.get('zip_code'),
            data.get('country')
        ])
        result = cursor.fetchone()
        conn.commit()
        
        if result[0]:
            return json_response(200, data={'message': result[1]})
        else:
            return json_response(400, error=result[1])
            
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Update profile error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/users', methods=['GET'])
@require_auth
@require_admin
def get_all_users():
    """Get all users (admin only)"""
    cursor = None
    conn = None
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id AS user_id,
                name,
                email,
                phone,
                is_admin,
                is_active,
                created_at
            FROM users
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )
        
        users = []
        for row in cursor.fetchall():
            columns = [desc[0] for desc in cursor.description]
            users.append(dict(zip(columns, row)))
        
        return json_response(200, data={'users': users, 'count': len(users)})
            
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Error fetching users: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()


@app.route('/api/users/<int:user_id>', methods=['DELETE'])
@require_auth
@require_admin
def delete_user(user_id):
    """Delete a user (admin only)."""
    cursor = None
    conn = None
    try:
        conn = ensure_service_connection('user_db')
        cursor = conn.cursor()
        cursor.execute('SELECT id, email, is_admin FROM users WHERE id = %s', (user_id,))
        existing = cursor.fetchone()
        if not existing:
            return json_response(404, error='User not found')

        if existing[2]:
            return json_response(400, error='Cannot delete admin user')

        cursor.execute('DELETE FROM users WHERE id = %s', (user_id,))
        conn.commit()
        add_monitor_event('user_deleted', service='user_db', details={'user_id': user_id, 'email': existing[1]})
        return json_response(200, data={'message': 'User deleted successfully', 'user_id': user_id})
    except Exception as e:
        safe_rollback(conn)
        logger.error(f"Delete user error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

# ========================
# PRODUCT SERVICE ENDPOINTS
# ========================

@app.route('/api/products', methods=['GET'])
def get_products():
    """Get all products"""
    try:
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 20, type=int)
        category = request.args.get('category')
        search = request.args.get('search')
        
        query = {'is_active': True}
        if category:
            query['category'] = category
        if search:
            query['$text'] = {'$search': search}

        products, total, read_role = read_products_with_mongo_fallback(query, page, limit)
        
        # Convert ObjectId to string
        for product in products:
            product['_id'] = str(product['_id'])
        
        return json_response(200, data={
            'products': products,
            'total': total,
            'page': page,
            'pages': (total + limit - 1) // limit,
            'read_source': read_role,
        })
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Error fetching products: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/products/<int:product_id>', methods=['GET'])
def get_product_details(product_id):
    """Get product details"""
    cursor = None
    try:
        product = None
        errors = []

        try:
            product = mongo_db.products.find_one({'_id': product_id})
        except Exception as e:
            errors.append(f"active: {str(e)}")

        if not product:
            try:
                active_role = active_db_endpoints.get('mongodb', 'primary')
                alternate_role = 'secondary' if active_role == 'primary' else 'primary'
                alt_db = get_mongo_db_for_role(alternate_role)
                product = alt_db.products.find_one({'_id': product_id})
            except Exception as e:
                errors.append(f"alternate: {str(e)}")

        if not product and errors:
            raise Exception('; '.join(errors))
        
        if not product:
            return json_response(404, error='Product not found')
        
        # Get inventory status
        cursor = inventory_db.cursor()
        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                stock_quantity,
                reserved_quantity,
                (stock_quantity - reserved_quantity) AS available_quantity
            FROM inventory
            WHERE product_id = %s
            """,
            (product_id,),
        )
        inventory_row = cursor.fetchone()
        cursor.close()
        cursor = None
        
        if inventory_row:
            product['inventory'] = {
                'stock_quantity': inventory_row[2],
                'reserved_quantity': inventory_row[3],
                'available_quantity': inventory_row[4]
            }
        
        product['_id'] = str(product['_id'])
        return json_response(200, data=product)
        
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Error fetching product: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/products', methods=['POST'])
@require_auth
@require_admin
def create_product():
    """Create a new product (admin only)"""
    try:
        data = request.get_json()

        if not data.get('name') or data.get('stock_reference_id') is None:
            return json_response(400, error='name and stock_reference_id are required')
        
        product_data = {
            'name': data.get('name'),
            'description': data.get('description'),
            'price': data.get('price'),
            'category': data.get('category'),
            'stock_reference_id': data.get('stock_reference_id'),
            'brand': data.get('brand'),
            'specifications': data.get('specifications', {}),
            'images': data.get('images', []),
            'is_active': True,
            'createdAt': datetime.utcnow(),
            'updatedAt': datetime.utcnow()
        }
        
        result = mongo_db.products.insert_one(product_data)

        inv_conn = ensure_service_connection('inventory_db')
        inv_cursor = inv_conn.cursor()
        inv_cursor.execute(
            """
            INSERT INTO inventory (product_id, product_name, stock_quantity, reserved_quantity, warehouse_location, reorder_level)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id)
            DO UPDATE SET
                product_name = EXCLUDED.product_name,
                stock_quantity = EXCLUDED.stock_quantity,
                reserved_quantity = EXCLUDED.reserved_quantity,
                warehouse_location = EXCLUDED.warehouse_location,
                reorder_level = EXCLUDED.reorder_level,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                int(product_data['stock_reference_id']),
                product_data['name'],
                int(data.get('initial_stock', 0) or 0),
                0,
                data.get('warehouse_location'),
                int(data.get('reorder_level', 10) or 10),
            ),
        )
        inv_conn.commit()
        inv_cursor.close()

        # Keep standalone secondary node synchronized when not running replica set.
        sync_mongo_standalone_data(['products'])
        add_monitor_event(
            'product_created',
            service='mongodb',
            details={
                'product_id': int(product_data['stock_reference_id']),
                'name': product_data['name'],
            },
        )
        
        return json_response(201, data={
            'product_id': result.inserted_id,
            'message': 'Product created successfully'
        })
    except Exception as e:
        logger.error(f"Error creating product: {str(e)}")
        return json_response(500, error=str(e))

# ========================
# ORDER SERVICE ENDPOINTS
# ========================

@app.route('/api/orders', methods=['POST'])
@require_auth
def create_order():
    """Create a new order with 2PC simulation"""
    cursor = None
    try:
        data = request.get_json()
        
        # Validate
        if 'items' not in data or not data['items']:
            return json_response(400, error='Order must contain items')
        
        items = data.get('items')
        shipping_address = data.get('shipping_address')
        payment_method = data.get('payment_method', 'CREDIT_CARD')

        # Ensure product names are present for persisted order_items rows.
        for item in items:
            if item.get('product_name'):
                continue
            try:
                product_doc = mongo_db.products.find_one(
                    {'_id': int(item.get('product_id'))},
                    {'name': 1},
                )
                if product_doc and product_doc.get('name'):
                    item['product_name'] = product_doc.get('name')
                else:
                    item['product_name'] = f"Product {item.get('product_id')}"
            except Exception:
                item['product_name'] = f"Product {item.get('product_id')}"
        
        # Phase 1: PREPARE - Check inventory for all items
        logger.info(f"2PC Phase 1 (PREPARE): Validating order for user {request.user_id}")
        
        for item in items:
            product_id = item.get('product_id')
            quantity = item.get('quantity')
            
            # Check stock availability
            cursor = inventory_db.cursor()
            call_db_routine(inventory_db, cursor, 'check_and_reserve_stock', [product_id, quantity])
            result = cursor.fetchone()
            cursor.close()
            
            if not result[0]:  # Not available
                # Rollback reservations
                for reserved_item in items[:items.index(item)]:
                    cursor = inventory_db.cursor()
                    call_db_routine(inventory_db, cursor, 'unreserve_stock', [reserved_item['product_id'], reserved_item['quantity']])
                    cursor.close()
                    inventory_db.commit()
                
                return json_response(400, error=f"Insufficient stock for product {product_id}: {result[2]}")
        
        # Phase 2: COMMIT - Create the order
        logger.info(f"2PC Phase 2 (COMMIT): Creating order for user {request.user_id}")
        
        import json
        cursor = order_db.cursor()
        items_json = json.dumps(items)
        call_db_routine(order_db, cursor, 'create_order', [request.user_id, items_json, shipping_address, payment_method])
        result = cursor.fetchone()
        order_db.commit()
        
        if result[0]:  # Order created
            order_id = result[1]
            
            # Deduct stock for each item
            for item in items:
                cursor = inventory_db.cursor()
                call_db_routine(inventory_db, cursor, 'deduct_stock', [item['product_id'], item['quantity'], str(order_id)])
                cursor.close()
                inventory_db.commit()
            
            logger.info(f"2PC completed: Order {order_id} created successfully")
            
            return json_response(201, data={
                'order_id': order_id,
                'message': result[2],
                'status': 'CONFIRMED'
            })
        else:
            return json_response(400, error=result[2])
            
    except Exception as e:
        safe_rollback(order_db)
        safe_rollback(inventory_db)
        logger.error(f"Order creation error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/orders/<int:order_id>', methods=['GET'])
@require_auth
def get_order(order_id):
    """Get order details"""
    cursor = None
    try:
        cursor = order_db.cursor()
        call_db_routine(order_db, cursor, 'get_order_details', [order_id])
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return json_response(404, error='Order not found')
        
        order = dict(zip(columns, result))
        
        # Get order items
        cursor = order_db.cursor()
        cursor.execute('SELECT id, product_id, product_name, quantity, unit_price, subtotal FROM order_items WHERE order_id = %s', (order_id,))
        items = []
        for row in cursor.fetchall():
            items.append({
                'id': row[0],
                'product_id': row[1],
                'product_name': row[2] or f"Product #{row[1]}",
                'quantity': row[3],
                'unit_price': float(row[4]),
                'subtotal': float(row[5])
            })
        cursor.close()
        
        order['total_price'] = float(order['total_price'])
        order['items'] = items
        
        return json_response(200, data=order)
        
    except Exception as e:
        safe_rollback(order_db)
        logger.error(f"Error fetching order: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/orders/user/<int:user_id>', methods=['GET'])
@require_auth
def get_user_orders(user_id):
    """Get all orders for a user"""
    cursor = None
    try:
        # Check authorization
        if request.user_id != user_id and not request.is_admin:
            return json_response(403, error='Unauthorized')
        
        limit = request.args.get('limit', 50, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        cursor = order_db.cursor()
        call_db_routine(order_db, cursor, 'get_user_orders', [user_id, limit, offset])
        
        orders = []
        for row in cursor.fetchall():
            columns = [desc[0] for desc in cursor.description]
            orders.append(dict(zip(columns, row)))
        
        cursor.close()
        
        return json_response(200, data={'orders': orders, 'count': len(orders)})
        
    except Exception as e:
        safe_rollback(order_db)
        logger.error(f"Error fetching user orders: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/orders', methods=['GET'])
@require_auth
@require_admin
def get_all_orders():
    """Get all orders (admin only)."""
    cursor = None
    try:
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)

        cursor = order_db.cursor()
        cursor.execute(
            """
            SELECT id, user_id, total_price, status, created_at
            FROM orders
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (limit, offset),
        )

        orders = []
        for row in cursor.fetchall():
            orders.append({
                'order_id': row[0],
                'user_id': row[1],
                'total_price': float(row[2]),
                'status': row[3],
                'created_at': row[4],
            })

        return json_response(200, data={'orders': orders, 'count': len(orders)})
    except Exception as e:
        safe_rollback(order_db)
        logger.error(f"Error fetching all orders: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()


@app.route('/api/orders/<int:order_id>', methods=['DELETE'])
@require_auth
@require_admin
def delete_order(order_id):
    """Delete an order and restore stock quantities (admin only)."""
    cursor = None
    try:
        cursor = order_db.cursor()
        cursor.execute('SELECT id FROM orders WHERE id = %s', (order_id,))
        existing = cursor.fetchone()
        if not existing:
            return json_response(404, error='Order not found')

        cursor.execute('SELECT product_id, quantity FROM order_items WHERE order_id = %s', (order_id,))
        items = cursor.fetchall()

        for product_id, qty in items:
            inv_cursor = inventory_db.cursor()
            try:
                inv_cursor.execute(
                    """
                    UPDATE inventory
                    SET stock_quantity = stock_quantity + %s,
                        reserved_quantity = GREATEST(0, reserved_quantity - %s),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE product_id = %s
                    """,
                    (qty, qty, product_id),
                )
                inventory_db.commit()
            finally:
                inv_cursor.close()

        cursor.execute('DELETE FROM orders WHERE id = %s', (order_id,))
        order_db.commit()
        add_monitor_event('order_deleted', service='order_db', details={'order_id': order_id, 'item_count': len(items)})
        return json_response(200, data={'message': 'Order deleted successfully', 'order_id': order_id})
    except Exception as e:
        safe_rollback(order_db)
        safe_rollback(inventory_db)
        logger.error(f"Delete order error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

# ========================
# INVENTORY SERVICE ENDPOINTS
# ========================

@app.route('/api/inventory/<int:product_id>', methods=['GET'])
def get_inventory(product_id):
    """Get inventory status for a product"""
    cursor = None
    try:
        cursor = inventory_db.cursor()
        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                stock_quantity,
                reserved_quantity,
                (stock_quantity - reserved_quantity) AS available_quantity,
                warehouse_location,
                reorder_level,
                last_restock_date
            FROM inventory
            WHERE product_id = %s
            """,
            (product_id,),
        )
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            return json_response(404, error='Product not found in inventory')
        
        inventory = {
            'product_id': result[0],
            'product_name': result[1],
            'stock_quantity': result[2],
            'reserved_quantity': result[3],
            'available_quantity': result[4],
            'warehouse_location': result[5],
            'reorder_level': result[6],
            'last_restock_date': result[7].isoformat() if result[7] else None
        }
        
        return json_response(200, data=inventory)
        
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Error fetching inventory: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/inventory', methods=['GET'])
@require_auth
@require_admin
def get_all_inventory():
    """Get full inventory list (admin only)."""
    cursor = None
    try:
        cursor = inventory_db.cursor()
        cursor.execute(
            """
            SELECT
                product_id,
                product_name,
                stock_quantity,
                reserved_quantity,
                (stock_quantity - reserved_quantity) AS available_quantity,
                warehouse_location,
                reorder_level,
                last_restock_date,
                updated_at
            FROM inventory
            ORDER BY product_id
            """
        )
        rows = cursor.fetchall()

        items = []
        for row in rows:
            items.append({
                'product_id': row[0],
                'product_name': row[1],
                'stock_quantity': row[2],
                'reserved_quantity': row[3],
                'available_quantity': row[4],
                'warehouse_location': row[5],
                'reorder_level': row[6],
                'last_restock_date': row[7].isoformat() if row[7] else None,
                'updated_at': row[8].isoformat() if row[8] else None,
            })

        return json_response(200, data={'inventory': items, 'count': len(items)})
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Error fetching full inventory: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

@app.route('/api/inventory/low-stock', methods=['GET'])
@require_auth
@require_admin
def get_low_stock():
    """Get items with low stock (admin only)"""
    cursor = None
    try:
        cursor = inventory_db.cursor()
        call_db_routine(inventory_db, cursor, 'get_low_stock_items')
        
        items = []
        for row in cursor.fetchall():
            items.append({
                'product_id': row[0],
                'product_name': row[1],
                'stock_quantity': row[2],
                'reorder_level': row[3],
                'status': row[4]
            })
        
        cursor.close()
        
        return json_response(200, data={'low_stock_items': items, 'count': len(items)})
        
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Error fetching low stock items: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/inventory/restock', methods=['POST'])
@require_auth
@require_admin
def restock_inventory():
    """Restock an item (admin only)"""
    cursor = None
    try:
        data = request.get_json()
        
        if 'product_id' not in data or 'quantity' not in data:
            return json_response(400, error='Product ID and quantity required')
        
        product_id = data.get('product_id')
        quantity = data.get('quantity')
        notes = data.get('notes')
        
        cursor = inventory_db.cursor()
        call_db_routine(inventory_db, cursor, 'restock_inventory', [product_id, quantity, notes])
        result = cursor.fetchone()
        inventory_db.commit()
        cursor.close()
        
        if result[0]:
            return json_response(200, data={
                'product_id': product_id,
                'new_stock': result[1],
                'message': result[2]
            })
        else:
            return json_response(400, error=result[2])
            
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Restock error: {str(e)}")
        return json_response(500, error=str(e))


@app.route('/api/inventory/<int:product_id>', methods=['PUT'])
@require_auth
@require_admin
def update_inventory_item(product_id):
    """Update inventory values for a product (admin only)."""
    cursor = None
    try:
        data = request.get_json() or {}
        updates = []
        values = []

        if 'stock_quantity' in data:
            updates.append('stock_quantity = %s')
            values.append(int(data.get('stock_quantity')))
        if 'reserved_quantity' in data:
            updates.append('reserved_quantity = %s')
            values.append(int(data.get('reserved_quantity')))
        if 'reorder_level' in data:
            updates.append('reorder_level = %s')
            values.append(int(data.get('reorder_level')))
        if 'warehouse_location' in data:
            updates.append('warehouse_location = %s')
            values.append(data.get('warehouse_location'))
        if 'product_name' in data:
            updates.append('product_name = %s')
            values.append(data.get('product_name'))

        if not updates:
            return json_response(400, error='No fields provided for update')

        updates.append('updated_at = CURRENT_TIMESTAMP')
        values.append(product_id)

        cursor = inventory_db.cursor()
        cursor.execute(
            f"UPDATE inventory SET {', '.join(updates)} WHERE product_id = %s RETURNING product_id, product_name, stock_quantity, reserved_quantity, reorder_level, warehouse_location, updated_at",
            tuple(values),
        )
        row = cursor.fetchone()
        if not row:
            return json_response(404, error='Inventory item not found')

        if row[3] > row[2]:
            return json_response(400, error='Reserved quantity cannot exceed stock quantity')

        inventory_db.commit()
        add_monitor_event(
            'inventory_updated',
            service='inventory_db',
            details={
                'product_id': row[0],
                'stock_quantity': row[2],
                'reserved_quantity': row[3],
                'reorder_level': row[4],
            },
        )
        return json_response(200, data={
            'message': 'Inventory updated successfully',
            'inventory': {
                'product_id': row[0],
                'product_name': row[1],
                'stock_quantity': row[2],
                'reserved_quantity': row[3],
                'reorder_level': row[4],
                'warehouse_location': row[5],
                'updated_at': row[6].isoformat() if row[6] else None,
            },
        })
    except Exception as e:
        safe_rollback(inventory_db)
        logger.error(f"Inventory update error: {str(e)}")
        return json_response(500, error=str(e))
    finally:
        if cursor:
            cursor.close()

# ========================
# ADMIN PANEL ENDPOINTS
# ========================

@app.route('/api/admin/status', methods=['GET'])
@require_auth
@require_admin
def admin_status():
    """Get system status (admin only)"""
    cursor = None
    try:
        status_data = {
            'timestamp': current_timestamp_iso(),
            'databases': db_status,
            'services': {}
        }
        
        # Keep dashboard responsive even if one service is down.
        user_count = None
        order_count = None
        product_count = None

        try:
            cursor = user_db.cursor()
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = TRUE')
            user_count = cursor.fetchone()[0]
            cursor.close()
        except Exception:
            safe_rollback(user_db)

        try:
            cursor = order_db.cursor()
            cursor.execute('SELECT COUNT(*) FROM orders')
            order_count = cursor.fetchone()[0]
            cursor.close()
        except Exception:
            safe_rollback(order_db)

        try:
            _, product_count, _ = read_products_with_mongo_fallback({'is_active': True}, 1, 1)
        except Exception:
            product_count = None
        
        status_data['services'] = {
            'active_users': user_count,
            'total_orders': order_count,
            'total_products': product_count,
            'degraded': any(v is None for v in (user_count, order_count, product_count)),
        }
        status_data['failover'] = {
            'active_db_endpoints': active_db_endpoints,
            'auto_failover': auto_failover_state,
        }
        
        return json_response(200, data=status_data)
        
    except Exception as e:
        safe_rollback(user_db)
        safe_rollback(order_db)
        logger.error(f"Error getting admin status: {str(e)}")
        return json_response(500, error=str(e))

@app.route('/api/admin/failover/simulate', methods=['POST'])
@require_auth
@require_admin
def simulate_failover():
    """Simulate database failover with node diagnostics (admin only)."""
    try:
        data = request.get_json() or {}
        database = data.get('database', 'order_db')

        mapping = {
            'user_db': (
                probe_postgres_node('user_db', 'primary', Config.USER_DB_HOST, Config.USER_DB_PORT, Config.USER_DB_NAME),
                probe_postgres_node('user_db', 'replica', Config.USER_DB_REPLICA_HOST, Config.USER_DB_REPLICA_PORT, Config.USER_DB_NAME),
            ),
            'order_db': (
                probe_postgres_node('order_db', 'primary', Config.ORDER_DB_HOST, Config.ORDER_DB_PORT, Config.ORDER_DB_NAME),
                probe_postgres_node('order_db', 'replica', Config.ORDER_DB_REPLICA_HOST, Config.ORDER_DB_REPLICA_PORT, Config.ORDER_DB_NAME),
            ),
            'inventory_db': (
                probe_postgres_node('inventory_db', 'primary', Config.INVENTORY_DB_HOST, Config.INVENTORY_DB_PORT, Config.INVENTORY_DB_NAME),
                probe_postgres_node('inventory_db', 'replica', Config.INVENTORY_DB_REPLICA_HOST, Config.INVENTORY_DB_REPLICA_PORT, Config.INVENTORY_DB_NAME),
            ),
        }

        if database not in mapping:
            return json_response(400, error=f'Invalid failover target: {database}')

        primary_status, replica_status = mapping[database]

        failover_state[database]['simulated_primary_down'] = True
        failover_state[database]['promoted'] = False
        failover_state[database]['last_action'] = 'simulate_failover'
        failover_state[database]['updated_at'] = current_timestamp_iso()

        primary_status['status'] = 'unhealthy'
        primary_status['role'] = 'primary (simulated failed)'
        primary_status['error'] = 'Simulated outage triggered by admin'

        can_promote = replica_status['status'] == 'healthy'
        recommendation = (
            'Promote replica to primary' if can_promote
            else 'Replica unavailable; cannot promote at this time'
        )

        logger.warning(f"FAILOVER SIMULATION target={database} primary={primary_status['status']} replica={replica_status['status']}")
        add_monitor_event('simulate_failover', service=database, details={
            'primary_status': primary_status.get('status'),
            'replica_status': replica_status.get('status'),
            'can_promote': can_promote,
        })

        return json_response(200, data={
            'message': f'Failover simulation completed for {database}',
            'target': database,
            'primary': primary_status,
            'replica': replica_status,
            'can_promote': can_promote,
            'recommendation': recommendation,
            'failover_state': failover_state[database],
            'note': 'Simulation marked primary as failed; use promote action to switch role in dashboard state',
            'timestamp': current_timestamp_iso()
        })
        
    except Exception as e:
        logger.error(f"Failover simulation error: {str(e)}")
        return json_response(500, error=str(e))


@app.route('/api/admin/failover/promote', methods=['POST'])
@require_auth
@require_admin
def promote_replica():
    """Promote a replica to primary and switch backend routing (admin only)."""
    try:
        data = request.get_json() or {}
        database = data.get('database', 'order_db')

        if database == 'mongodb':
            ok, message, promoted_status = execute_mongo_promote_secondary()
            if not ok:
                return json_response(409, error=message, data={'replica': promoted_status})

            add_monitor_event('manual_promote', service='mongodb', node='secondary', details={'message': message})

            return json_response(200, data={
                'message': 'Mongo secondary promoted',
                'target': 'mongodb',
                'replica': promoted_status,
                'active_db_endpoint': active_db_endpoints['mongodb'],
                'note': 'Mongo step-up executed on secondary.',
                'timestamp': current_timestamp_iso(),
            })

        if database not in ('user_db', 'order_db', 'inventory_db'):
            return json_response(400, error=f'Invalid failover target: {database}')

        ok, message, promoted_status = execute_pg_promote(database)
        if not ok:
            return json_response(409, error=message, data={'replica': promoted_status})

        failover_state[database]['simulated_primary_down'] = False
        failover_state[database]['promoted'] = True
        failover_state[database]['last_action'] = 'promote_replica_actual'
        failover_state[database]['updated_at'] = current_timestamp_iso()
        add_monitor_event('manual_promote', service=database, node='replica', details={'message': message})

        return json_response(200, data={
            'message': f'Replica promoted for {database}',
            'target': database,
            'replica': promoted_status,
            'failover_state': failover_state[database],
            'active_db_endpoint': active_db_endpoints[database],
            'note': 'Promotion executed on PostgreSQL and backend routing switched to promoted node.',
            'timestamp': current_timestamp_iso(),
        })
    except Exception as e:
        logger.error(f"Replica promotion error: {str(e)}")
        return json_response(500, error=str(e))


@app.route('/api/admin/failover/node/fail', methods=['POST'])
@require_auth
@require_admin
def fail_node():
    data = request.get_json() or {}
    service = data.get('service')
    node = data.get('node')
    ok, message = fail_node_actual(service, node)
    if not ok:
        return json_response(400, error=message)
    return json_response(200, data={
        'message': message,
        'service': service,
        'node': node,
        'timestamp': current_timestamp_iso(),
    })


@app.route('/api/admin/failover/node/start', methods=['POST'])
@require_auth
@require_admin
def start_node():
    data = request.get_json() or {}
    service = data.get('service')
    node = data.get('node')
    ok, message = start_node_actual(service, node)
    if not ok:
        return json_response(400, error=message)

    # If primary is started again, do not auto-switch PostgreSQL routing back.
    # A recovered original primary can be stale after replica promotion.
    if service in ('user_db', 'order_db', 'inventory_db') and node == 'primary':
        cfg = service_config(service)
        primary_probe = probe_postgres_node(service, 'primary', cfg['primary'][0], cfg['primary'][1], cfg['db_name'])
        if primary_probe.get('status') == 'healthy' and primary_probe.get('is_in_recovery') is False:
            with state_lock:
                failover_state[service]['last_action'] = 'primary_started_no_auto_failback'
                failover_state[service]['updated_at'] = current_timestamp_iso()
            message = (
                f"{message}. Primary started; backend kept on current writer to prevent stale reads. "
                f"Manual re-sync/switchover is required before routing back."
            )

    if service == 'mongodb' and node == 'primary':
        primary_probe = probe_mongo_node('primary', Config.MONGO_HOST, Config.MONGO_PORT)
        if primary_probe.get('status') == 'healthy':
            try:
                reconnect_mongo_db('primary')
                with state_lock:
                    active_db_endpoints['mongodb'] = 'primary'
                message = f"{message}. Mongo active endpoint switched back to primary"
            except Exception as reconnect_error:
                logger.warning(f"Mongo primary restore routing switch failed: {str(reconnect_error)}")

    return json_response(200, data={
        'message': message,
        'service': service,
        'node': node,
        'timestamp': current_timestamp_iso(),
    })


@app.route('/api/admin/failover/node/promote', methods=['POST'])
@require_auth
@require_admin
def promote_node():
    data = request.get_json() or {}
    service = data.get('service')
    node = data.get('node')

    if service == 'mongodb' and node == 'secondary':
        ok, message, status = execute_mongo_promote_secondary()
        if not ok:
            return json_response(409, error=message, data={'status': status})
        return json_response(200, data={
            'message': message,
            'service': service,
            'node': node,
            'status': status,
            'timestamp': current_timestamp_iso(),
        })

    if service in ('user_db', 'order_db', 'inventory_db') and node == 'replica':
        ok, message, status = execute_pg_promote(service)
        if not ok:
            return json_response(409, error=message, data={'status': status})
        with state_lock:
            failover_state[service]['promoted'] = True
            failover_state[service]['simulated_primary_down'] = False
            failover_state[service]['last_action'] = 'manual_promote_node'
            failover_state[service]['updated_at'] = current_timestamp_iso()
        return json_response(200, data={
            'message': message,
            'service': service,
            'node': node,
            'status': status,
            'timestamp': current_timestamp_iso(),
        })

    return json_response(400, error='Promote is supported for postgres replicas and mongodb secondary only')


@app.route('/api/admin/failover/reset', methods=['POST'])
@require_auth
@require_admin
def reset_failover_environment():
    ok, details = reset_all_databases_actual()
    if not ok:
        return json_response(500, error='Reset failed', data=details)

    # Rebuild DB connections for backend after container reset.
    initialize_databases()
    add_monitor_event('environment_reset', details={'result': 'completed'})

    return json_response(200, data={
        'message': 'All DB containers reset to normal baseline (this recreates data volumes).',
        'details': details,
        'active_db_endpoints': active_db_endpoints,
        'timestamp': current_timestamp_iso(),
    })


@app.route('/api/admin/failover/monitor/status', methods=['GET'])
@require_auth
@require_admin
def failover_monitor_status():
    passive_failback_to_primary()
    return json_response(200, data={
        'auto_failover': auto_failover_state,
        'active_db_endpoints': active_db_endpoints,
        'events': list(reversed(monitor_events[-100:])),
    })


@app.route('/api/admin/failover/monitor/start', methods=['POST'])
@require_auth
@require_admin
def failover_monitor_start():
    data = request.get_json() or {}
    interval_seconds = data.get('interval_seconds', 5)
    started = start_auto_failover(interval_seconds)
    return json_response(200, data={
        'started': started,
        'auto_failover': auto_failover_state,
        'events': list(reversed(monitor_events[-100:])),
    })


@app.route('/api/admin/failover/monitor/stop', methods=['POST'])
@require_auth
@require_admin
def failover_monitor_stop():
    stopped = stop_auto_failover()
    return json_response(200, data={
        'stopped': stopped,
        'auto_failover': auto_failover_state,
        'events': list(reversed(monitor_events[-100:])),
    })


@app.route('/api/admin/failover/monitor/config', methods=['PUT'])
@require_auth
@require_admin
def failover_monitor_update_config():
    data = request.get_json() or {}
    interval_seconds = data.get('interval_seconds')
    if interval_seconds is None:
        return json_response(400, error='interval_seconds is required')

    try:
        interval_seconds = max(2, int(interval_seconds))
    except Exception:
        return json_response(400, error='interval_seconds must be an integer >= 2')

    with state_lock:
        auto_failover_state['interval_seconds'] = interval_seconds
        auto_failover_state['last_event'] = {
            'action': 'monitor_interval_updated',
            'timestamp': current_timestamp_iso(),
            'interval_seconds': interval_seconds,
        }

    add_monitor_event('monitor_interval_updated', details={'interval_seconds': interval_seconds})
    return json_response(200, data={
        'message': 'Monitor interval updated',
        'auto_failover': auto_failover_state,
    })


@app.route('/api/admin/failover/monitor/events', methods=['GET'])
@require_auth
@require_admin
def failover_monitor_events():
    limit = request.args.get('limit', 100, type=int)
    limit = max(1, min(limit, MAX_MONITOR_EVENTS))
    return json_response(200, data={
        'events': list(reversed(monitor_events[-limit:])),
        'count': min(limit, len(monitor_events)),
    })

# ========================
# ERROR HANDLERS
# ========================

@app.errorhandler(404)
def not_found(error):
    return json_response(404, error='Resource not found')

@app.errorhandler(500)
def internal_error(error):
    return json_response(500, error='Internal server error')

# ========================
# MAIN
# ========================

if __name__ == '__main__':
    # Ensure logs directory exists
    os.makedirs(os.path.join(CURRENT_DIR, 'logs'), exist_ok=True)
    
    # Initialize databases
    initialize_databases()
    
    # Run Flask app
    logger.info("Starting E-Commerce Distributed System")
    app.run(host='0.0.0.0', port=5000, debug=False)

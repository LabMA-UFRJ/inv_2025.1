import oracledb
import os
import logging
import csv
import time
import subprocess
import re
import sys
import uuid
from datetime import datetime
import platform
import struct
import argparse
#from notifier import Notifier

# --- Configuration ---
DB_USER = "GABRIEL_M"
DB_PASSWORD = "m0ulrlyc"
db_name = "tabuas"
port = "1521"
DB_DSN = f"192.168.10.111:{port}/{db_name}"

block_size = 5000000

CONFIG_FILE = "script\pipeline_tasks.csv"
VERBOSE_LOG_FILE = "pipeline_run.log"
SUMMARY_LOG_FILE = "pipeline_summary.csv"

def get_connection(lib_dir=None):
    """Attempt to connect using thin mode first. If the server version is
    incompatible with thin mode, try to initialize the Oracle Instant Client
    (thick mode) and reconnect.

    Args:
        lib_dir (str, optional): Path to Oracle Instant Client directory.
                                 If provided, will be used for thick mode initialization.

    Returns a live oracledb connection or raises the original exception with
    additional diagnostics.
    """
    try:
        logging.info("Attempting to connect in thin mode to %s", DB_DSN)
        conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
        logging.info("Connected using python-oracledb thin mode.")
        return conn
    except oracledb.DatabaseError as e:
        msg = str(e)
        # Detect the common incompatibility message that indicates the
        # server version doesn't support thin mode.
        if 'not supported by python-oracledb in thin mode' in msg or 'thin mode' in msg and 'not supported' in msg:
            logging.warning("Server appears incompatible with thin mode. Will attempt thick mode (Oracle Instant Client).")
            # Try to initialize the Oracle Instant Client. If the client is
            # installed and on PATH (Windows) or LD_LIBRARY_PATH (Unix), the
            # call without lib_dir should work. If not, a helpful error will
            # be logged and the exception re-raised.
            try:
                logging.info("Calling oracledb.init_oracle_client() to enable thick mode.")
                # Allow users to specify the Instant Client directory via CLI arg or env var
                client_lib_dir = lib_dir or os.environ.get('ORACLE_CLIENT_LIB_DIR')
                if client_lib_dir:
                    logging.info("Using ORACLE_CLIENT_LIB_DIR=%s", client_lib_dir)
                    oracledb.init_oracle_client(lib_dir=client_lib_dir)
                else:
                    oracledb.init_oracle_client()
            except Exception as init_err:
                logging.error("Failed to initialize Oracle Instant Client: %s", init_err)
                # Provide extra diagnostics for DPI-1047 (missing library / bitness mismatch)
                try:
                    err_str = str(init_err)
                except Exception:
                    err_str = None
                if err_str and 'DPI-1047' in err_str or err_str and 'Cannot locate a' in err_str:
                    # Log platform and Python/oracledb bitness to help the user diagnose
                    py_bits = struct.calcsize('P') * 8
                    logging.error("Detected DPI-1047: possible missing Oracle Client or bitness mismatch.")
                    logging.error("Python executable: %s", sys.executable)
                    logging.error("Python version: %s", platform.python_version())
                    logging.error("Python architecture (bits): %s", py_bits)
                    try:
                        logging.error("python-oracledb version: %s", getattr(oracledb, '__version__', 'unknown'))
                    except Exception:
                        pass
                    logging.error("ORACLE_CLIENT_LIB_DIR=%s", os.environ.get('ORACLE_CLIENT_LIB_DIR'))
                    logging.error("Reminder: install the 64-bit Oracle Instant Client that matches your Python bitness and add it to PATH, or set ORACLE_CLIENT_LIB_DIR to the Instant Client folder.")
                logging.error(
                    "To use thick mode you must install Oracle Instant Client and either add its directory to PATH (Windows) or set LD_LIBRARY_PATH (Unix), or pass lib_dir to init_oracle_client()."
                )
                # Re-raise original (or new) exception so caller knows connect failed
                raise

            # Retry connection in thick mode
            try:
                conn = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
                logging.info("Connected using python-oracledb thick mode (Instant Client).")
                return conn
            except Exception as thick_exc:
                logging.error("Failed to connect in thick mode: %s", thick_exc)
                raise
        else:
            # Some other DatabaseError — re-raise for the caller to handle
            raise

# --- Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(VERBOSE_LOG_FILE), logging.StreamHandler(sys.stdout)]
)

SUMMARY_HEADER = ['run_id', 'task_name', 'start_time', 'end_time', 'duration_seconds', 'status', 'error_message']

def log_summary(run_id, task_name, start_time, end_time, status, error_msg=None):
    """Writes a summary of a task's execution to a local CSV file."""
    file_exists = os.path.exists(SUMMARY_LOG_FILE)
    duration = end_time - start_time
    log_entry = {
        'run_id': run_id,
        'task_name': task_name,
        'start_time': datetime.fromtimestamp(start_time).isoformat(),
        'end_time': datetime.fromtimestamp(end_time).isoformat(),
        'duration_seconds': round(duration, 2),
        'status': status,
        'error_message': str(error_msg).replace('\n', ' ').replace('\r', '') if error_msg else ''
    }
    try:
        with open(SUMMARY_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=SUMMARY_HEADER)
            if not file_exists or os.path.getsize(SUMMARY_LOG_FILE) == 0:
                writer.writeheader()
            writer.writerow(log_entry)
    except Exception as e:
        logging.error(f"Failed to write to summary log file: {e}")

def execute_sql_task(cursor, script_path):
    """
    Reads a SQL file and executes its statements, correctly handling the
    different semicolon requirements for SQL DDL/DML and PL/SQL blocks.
    """
    logging.info(f"Executing SQL script: {script_path}")
    try:
        with open(script_path, 'r', encoding='utf-8') as f:
            # Split the entire file by the forward slash on its own line
            sql_commands = f.read().split(r'//')

        for command in sql_commands:
            command = command.strip()
            if not command:
                continue

            # Check if the block is PL/SQL (starts with BEGIN or DECLARE)
            # PL/SQL blocks REQUIRE a trailing semicolon to be valid.
            is_plsql = command.upper().startswith(('BEGIN', 'DECLARE'))

            # If it's NOT a PL/SQL block AND it ends with a semicolon,
            # we must remove the semicolon to prevent ORA-00911.
            if not is_plsql and command.endswith(';'):
                command = command[:-1]

            logging.debug(f"Executing final command: {command[:200]}...")
            cursor.execute(command)
            logging.debug("Command executed successfully.")

        logging.info(f"Successfully finished SQL script: {script_path}")

    except oracledb.DatabaseError as e:
        error, = e.args
        logging.error(f"A database error occurred in script: {script_path}")
        logging.error(f"Oracle Error Code: {error.code}")
        logging.error(f"Oracle Error Message: {error.message}")
        logging.error(f"Error occurred near offset: {error.offset}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while processing the SQL script: {script_path}")
        raise e
    
def execute_python_task(script_path):
    """Executes a Python script as a separate process."""
    logging.info(f"Executing Python script: {script_path}")
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    if result.returncode != 0:
        logging.error(f"Python script stdout:\n{result.stdout}")
        logging.error(f"Python script stderr:\n{result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
    logging.info(f"Successfully finished Python script: {script_path}")
    logging.info(f"Python script output:\n{result.stdout}")

def main(lib_dir=None):
    """Main pipeline execution function.
    
    Args:
        lib_dir (str, optional): Path to Oracle Instant Client directory.
    """
    run_id = str(uuid.uuid4())
    logging.info(f"--- Starting Pipeline Run ID: {run_id} ---")

    #notifier = Notifier()

    try:
        with open(CONFIG_FILE, mode='r', newline='', encoding='utf-8') as f:
            tasks = list(csv.DictReader(f))
        tasks.sort(key=lambda x: int(x['task_order']))
    except FileNotFoundError:
        error_msg = f"Configuration file not found: {CONFIG_FILE}"
        logging.error(error_msg)
        """ notifier.send_alert(
            subject="CRITICAL: Pipeline Configuration File Missing",
            message_body=f"Run ID: {run_id}\n\n{error_msg}"
        ) """
        return

    connection = None
    pipeline_failed = False
    try:
        # Establish a connection (thin mode first, fallback to thick if needed)
        connection = get_connection(lib_dir=lib_dir)
        with connection.cursor() as cursor:
            for task in tasks:
                if task['enabled'] != '1':
                    logging.info(f"Skipping disabled task: {task['task_name']}")
                    continue

                task_name = task['task_name']
                script_path = task['script_path']
                task_type = task['task_type'].upper()
                start_time = time.time()

                try:
                    if task_type == 'SQL':
                        execute_sql_task(cursor, script_path)
                    elif task_type == 'PYTHON':
                        execute_python_task(script_path)
                    else:
                        raise ValueError(f"Unknown task type '{task_type}' for task '{task_name}'")

                    end_time = time.time()
                    log_summary(run_id, task_name, start_time, end_time, 'SUCCESS')
                    connection.commit()

                    subject = f"INFO: Pipeline Step Sucess: {task_name}"
                    message_body = (
                        f"Run ID: {run_id}\n"
                        f"Task: {task_name}\n"
                        f"Status: SUCESS\n\n"
                        f"Time taken:{end_time - start_time}"
                    )
                    """ notifier.send_alert(
                                    subject=subject,
                                    message_body=message_body) """
                except Exception as e:
                    pipeline_failed = True
                    end_time = time.time()
                    error_details = str(e)
                    logging.error(f"!!! TASK FAILED: {task_name} !!!")
                    logging.error(f"Error details: {error_details}", exc_info=False)
                    log_summary(run_id, task_name, start_time, end_time, 'FAILURE', e)

                    # Use the notifier instance to send the alert
                    subject = f"Pipeline Task Failed: {task_name}"
                    message_body = (
                        f"Run ID: {run_id}\n"
                        f"Task: {task_name}\n"
                        f"Status: FAILURE\n\n"
                        f"Error Details:\n{error_details}"
                    )
                    #notifier.send_alert(subject, message_body)

                    break # Stop pipeline on first failure

    except oracledb.DatabaseError as e:
        pipeline_failed = True
        error_msg = f"A critical database connection error occurred: {e}"
        logging.critical(error_msg)
        """ notifier.send_alert(
            subject="CRITICAL: Pipeline Database Connection Failure",
            message_body=f"Run ID: {run_id}\n\nThe pipeline could not connect to the database.\n\nError:\n{error_msg}"
        ) """

    except Exception as e:
        pipeline_failed = True
        error_msg = f"An unexpected orchestrator error occurred: {e}"
        logging.critical(error_msg, exc_info=True)
        
        """ notifier.send_alert(
            subject="CRITICAL: Pipeline Orchestrator Failure",
            message_body=f"Run ID: {run_id}\n\nThe main script encountered an unexpected error.\n\nError:\n{error_msg}"
        ) """

    finally:
        if connection:
            if pipeline_failed:
                logging.warning("Rolling back any uncommitted transaction due to failure.")
                connection.rollback()
            connection.close()

    if pipeline_failed:
        logging.error(f"--- Pipeline Run {run_id} FAILED. ---")
    else:
        logging.info(f"--- Pipeline Run {run_id} COMPLETED SUCCESSFULLY. ---")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Oracle Database pipeline runner. Attempts to connect in thin mode, falls back to thick mode if needed.'
    )
    parser.add_argument(
        '--lib-dir',
        type=str,
        default=None,
        help=r'Path to Oracle Instant Client directory for thick mode (e.g., C:\oracle\instantclient_21_9). If not provided, ORACLE_CLIENT_LIB_DIR env var will be checked.'
    )
    args = parser.parse_args()
    main(lib_dir=args.lib_dir)

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
from notifier import Notifier

# --- Configuration ---
DB_USER = ""
DB_PASSWORD = ""
db_name = "tabuas"
port = "1521"
DB_DSN = f"192.168.10.103:{port}/{db_name}"

block_size = 5000000

CONFIG_FILE = r"pipeline_tasks.csv"
VERBOSE_LOG_FILE = "pipeline_run.log"
SUMMARY_LOG_FILE = "pipeline_summary.csv"

# --- Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(VERBOSE_LOG_FILE), logging.StreamHandler(sys.stdout)]
)

SUMMARY_HEADER = ['run_id', 'task_name', 'start_time', 'end_time', 'duration_seconds', 'status', 'error_message']

oracledb.init_oracle_client(r"C:\instantclient_23_8")

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

def main():
    """Main pipeline execution function."""
    run_id = str(uuid.uuid4())
    logging.info(f"--- Starting Pipeline Run ID: {run_id} ---")

    notifier = Notifier()

    try:
        with open(CONFIG_FILE, mode='r', newline='', encoding='utf-8') as f:
            tasks = list(csv.DictReader(f))
        tasks.sort(key=lambda x: int(x['task_order']))
    except FileNotFoundError:
        error_msg = f"Configuration file not found: {CONFIG_FILE}"
        logging.error(error_msg)
        notifier.send_alert(
            subject="CRITICAL: Pipeline Configuration File Missing",
            message_body=f"Run ID: {run_id}\n\n{error_msg}"
        )
        return

    connection = None
    pipeline_failed = False
    try:
        connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=DB_DSN)
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
                    notifier.send_alert(
                                    subject=subject,
                                    message_body=message_body)
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
                    notifier.send_alert(subject, message_body)

                    break # Stop pipeline on first failure

    except oracledb.DatabaseError as e:
        pipeline_failed = True
        error_msg = f"A critical database connection error occurred: {e}"
        logging.critical(error_msg)
        notifier.send_alert(
            subject="CRITICAL: Pipeline Database Connection Failure",
            message_body=f"Run ID: {run_id}\n\nThe pipeline could not connect to the database.\n\nError:\n{error_msg}"
        )

    except Exception as e:
        pipeline_failed = True
        error_msg = f"An unexpected orchestrator error occurred: {e}"
        logging.critical(error_msg, exc_info=True)
        notifier.send_alert(
            subject="CRITICAL: Pipeline Orchestrator Failure",
            message_body=f"Run ID: {run_id}\n\nThe main script encountered an unexpected error.\n\nError:\n{error_msg}"
        )

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



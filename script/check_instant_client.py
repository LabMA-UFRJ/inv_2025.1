"""Small helper to test Oracle Instant Client initialization.

Run this script after you install Instant Client or set ORACLE_CLIENT_LIB_DIR.
It will attempt to call oracledb.init_oracle_client(lib_dir=...) (if the env var
is set) and report the result and basic diagnostics.
"""
import os
import sys
import logging
import platform
import struct

try:
    import oracledb
except Exception as e:
    print("python-oracledb import failed:", e)
    sys.exit(2)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Running Instant Client check helper")
    lib_dir = os.environ.get('ORACLE_CLIENT_LIB_DIR')
    if lib_dir:
        logging.info("ORACLE_CLIENT_LIB_DIR is set: %s", lib_dir)
    else:
        logging.info("ORACLE_CLIENT_LIB_DIR is not set; init_oracle_client() will be called without lib_dir")

    logging.info("Python executable: %s", sys.executable)
    logging.info("Python version: %s", platform.python_version())
    logging.info("Python bits: %s", struct.calcsize('P') * 8)
    logging.info("python-oracledb version: %s", getattr(oracledb, '__version__', 'unknown'))

    try:
        if lib_dir:
            oracledb.init_oracle_client(lib_dir=lib_dir)
        else:
            oracledb.init_oracle_client()
        logging.info("init_oracle_client() succeeded — Instant Client is available and initialized.")
        print("SUCCESS: Instant Client initialized.")
        sys.exit(0)
    except Exception as e:
        logging.error("init_oracle_client() failed: %s", e)
        print("FAIL: init_oracle_client() failed:")
        print(e)
        # Provide pointer to DPI-1047 troubleshooting
        if 'DPI-1047' in str(e) or 'Cannot locate' in str(e):
            print("Possible causes: Instant Client missing or bitness mismatch (need 64-bit Instant Client for 64-bit Python).")
        sys.exit(3)

if __name__ == '__main__':
    main()

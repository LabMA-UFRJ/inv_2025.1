## Oracle Instant Client (thick mode) requirement

This project uses python-oracledb and attempts to connect using "thin" mode by
default. Some Oracle Database server versions are not compatible with thin
mode and will raise an error like:

    connections to this database server version are not supported by python-oracledb in thin mode

To allow the script to connect in those cases the pipeline will try to
initialize the Oracle Instant Client (thick mode) automatically. You must have
the Oracle Instant Client installed on your machine and its directory available
on PATH (Windows) or LD_LIBRARY_PATH (Linux/macOS). Alternatively, you can set
the environment variable `ORACLE_CLIENT_LIB_DIR` to the Instant Client folder
path so the script can call `oracledb.init_oracle_client(lib_dir=...)`.

On Windows:

1. Download Instant Client Basic (matching your OS/architecture) from Oracle:
   https://www.oracle.com/database/technologies/instant-client.html
2. Unzip the Instant Client files to a folder, for example `C:\oracle\instantclient_21_9`.
3. Add that folder to your PATH, or set `ORACLE_CLIENT_LIB_DIR` to that folder.

After installing Instant Client you can re-run the pipeline. If thick mode is
used the script will log that it connected using thick mode.

PowerShell quick steps to test and set up Instant Client

**Using the --lib-dir CLI flag (recommended)**

Instead of setting environment variables, you can pass the Instant Client path
directly on the command line:

    python .\script\run_all.py --lib-dir 'C:\oracle\instantclient_21_9'

This way, you don't need to change system PATH or environment variables.

**Using ORACLE_CLIENT_LIB_DIR environment variable (alternative)**

Or, if you prefer to set an environment variable:

1) If you already installed Instant Client but didn't set env vars, test the helper:

    # Run the included helper that tries to initialize Instant Client
    python .\script\check_instant_client.py

2) If the helper fails with DPI-1047, set `ORACLE_CLIENT_LIB_DIR` to the folder where you
   unzipped the Instant Client. Example (temporary for the session):

    # PowerShell (temporary for this session)
    $env:ORACLE_CLIENT_LIB_DIR = 'C:\oracle\instantclient_21_9'

   Or add the folder to PATH permanently (requires admin privileges):

    # PowerShell (set permanently for current user)
    [Environment]::SetEnvironmentVariable('Path', $env:Path + ';C:\oracle\instantclient_21_9', 'User')

3) Re-run the helper to confirm initialization succeeded:

    python .\script\check_instant_client.py

Notes:
- Make sure you install the 64-bit Instant Client when running 64-bit Python.
- If you still see errors, run the helper and attach its printed output when asking for help.
```.
├── New/                        # Modelo para análises. Inclui tipos de entrada
│   ├── inv/
│   │   ├── analises_inv_cpf_uni
│   │   ├── analises_inv_ind
│   │   └── analises_inv_reg
│   ├── mor/
│   │   ├── analises_mor_cpf_uni
│   │   └── [...]
│   └── sob/
│       ├── analises_sob_cpf_uni
│       └── [...]
└── Old/                        # Modelo anterior. ~ [...]
    ├── inv/
    ├── mor/
    └── sob/

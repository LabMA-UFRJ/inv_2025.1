import pandas as pd
from typing import Optional

def extract_csv_from_table(connection, table_name: str, output_path: Optional[str] = None) -> pd.DataFrame:
    """
    Extract data from a database table and save as CSV.
    
    Args:
        connection: Database connection object
        table_name: Name of the table to extract
        output_path: Optional path to save CSV file
    
    Returns:
        DataFrame with the extracted data
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, connection)
    
    if output_path:
        df.to_csv(output_path, index=False)
        print(f"CSV saved to {output_path}")
    
    return df
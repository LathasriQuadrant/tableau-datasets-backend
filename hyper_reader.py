from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd
import os
import sys

def clean_table_name(name: str) -> str:
    """
    Clean table name from various formats.
    
    Examples:
    - "customers" → "customers"
    - "customers.csv" → "customers"
    - "customers.csv_8DD21EEE" → "customers"
    - "Extract" → "Extract"
    """
    # First remove .csv extension if present
    if name.endswith(".csv"):
        name = name[:-4]
    
    # Then remove any _XXXXXXXX suffix (from exports)
    if "_" in name:
        base = name.split("_")[0]
        return base
    
    return name

def normalize(name: str) -> str:
    """Remove quotes from names"""
    return name.strip('"')

def is_default_schema(schema_name: str) -> bool:
    """Check if schema is a default/empty schema that should be ignored"""
    # Common default schema names that we should skip
    default_schemas = ["public", "information_schema", "pg_catalog", "sys"]
    return schema_name.lower() in default_schemas

def extract_hyper_to_csv(hyper_path: str, output_dir: str, workbook_name: str):
    csv_files = []
    tables = []

    print(f"\n[HYPER] Starting extraction from: {hyper_path}")
    print(f"[HYPER] Output directory: {output_dir}")
    sys.stdout.flush()

    try:
        with HyperProcess(
            telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU
        ) as hyper:
            print("[HYPER] HyperProcess initialized")
            sys.stdout.flush()

            with Connection(endpoint=hyper.endpoint, database=hyper_path) as conn:
                print("[HYPER] Connection established")
                sys.stdout.flush()

                # Get all schemas
                schema_list = conn.catalog.get_schema_names()
                print(f"[HYPER] Found {len(schema_list)} schema(s)")
                sys.stdout.flush()

                for schema_idx, schema in enumerate(schema_list):
                    schema_name = normalize(str(schema))
                    print(f"\n[SCHEMA {schema_idx}] Name: '{schema_name}'")
                    sys.stdout.flush()

                    # Get all tables in this schema
                    table_list = conn.catalog.get_table_names(schema)
                    print(f"[SCHEMA {schema_idx}] Found {len(table_list)} table(s)")
                    sys.stdout.flush()

                    for table_idx, table in enumerate(table_list):
                        raw_name = normalize(str(table.name))
                        table_name = clean_table_name(raw_name)
                        
                        print(f"  [TABLE {table_idx}] Raw: '{raw_name}' → Cleaned: '{table_name}'")
                        sys.stdout.flush()

                        table_info = {
                            "schema": schema_name,
                            "table": table_name,
                            "exported": False
                        }

                        try:
                            # Get column definitions
                            table_def = conn.catalog.get_table_definition(table)
                            columns = [normalize(str(c.name)) for c in table_def.columns]
                            print(f"    [COLUMNS] {len(columns)} columns found")
                            sys.stdout.flush()

                            # Execute query to get data
                            query = f'SELECT * FROM "{schema_name}"."{raw_name}"'
                            print(f"    [QUERY] {query}")
                            sys.stdout.flush()
                            
                            rows = conn.execute_list_query(query)
                            print(f"    [ROWS] {len(rows)} rows fetched")
                            sys.stdout.flush()

                            # Create DataFrame and save to CSV
                            df = pd.DataFrame(rows, columns=columns)
                            print(f"    [DATAFRAME] Created: {df.shape}")
                            sys.stdout.flush()

                            # Generate CSV filename based on schema and table name
                            # If schema is default (Extract, Public, etc), just use table name
                            # Otherwise use schema_tablename format
                            if is_default_schema(schema_name) or schema_name.lower() == "extract":
                                csv_name = f"{table_name}.csv"
                            else:
                                csv_name = f"{schema_name}_{table_name}.csv"
                            
                            csv_path = os.path.join(output_dir, csv_name)
                            
                            df.to_csv(csv_path, index=False)
                            csv_files.append(csv_path)
                            
                            table_info["exported"] = True
                            table_info["csv_filename"] = csv_name  # Store actual filename used
                            print(f"    [SAVED] {csv_name}")
                            sys.stdout.flush()

                        except Exception as table_error:
                            table_info["error"] = str(table_error)
                            print(f"    [ERROR] {str(table_error)}")
                            import traceback
                            traceback.print_exc()
                            sys.stdout.flush()

                        tables.append(table_info)

        print(f"\n[HYPER] MULTI TABLE VERSION ACTIVE")
        print(f"[HYPER] Total CSVs: {len(csv_files)}")
        print(f"[HYPER] Total tables: {len(tables)}")
        print(f"[HYPER] Successfully exported: {sum(1 for t in tables if t.get('exported'))}")
        sys.stdout.flush()

    except Exception as hyper_error:
        print(f"\n[HYPER] CRITICAL ERROR: {str(hyper_error)}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        raise

    return {
        "csv_files": csv_files,
        "tables": tables
    }
from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd
import os
import sys

def clean_table_name(name: str) -> str:
    # customers.csv_8DD21EEE... → customers
    base = name.split("_")[0]
    return base.replace(".csv", "")

def normalize(name: str) -> str:
    return name.strip('"')

def extract_hyper_to_csv(hyper_path: str, output_dir: str, workbook_name: str):
    csv_files = []
    tables = []

    print(f"\n{'='*60}")
    print(f"[HYPER START] File: {hyper_path}")
    print(f"[HYPER START] Output dir: {output_dir}")
    print(f"{'='*60}\n")

    try:
        print("[HYPER] Initializing HyperProcess...")
        sys.stdout.flush()
        
        with HyperProcess(
            telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU
        ) as hyper:
            print("[HYPER] HyperProcess initialized ✓")
            sys.stdout.flush()

            print("[HYPER] Establishing connection...")
            sys.stdout.flush()
            
            with Connection(endpoint=hyper.endpoint, database=hyper_path) as conn:
                print("[HYPER] Connection established ✓")
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
                        
                        print(f"\n  [TABLE {table_idx}] Raw name: '{raw_name}'")
                        print(f"  [TABLE {table_idx}] Cleaned name: '{table_name}'")
                        sys.stdout.flush()

                        table_info = {
                            "schema": schema_name,
                            "table": table_name,
                            "exported": False
                        }

                        try:
                            # Get table definition
                            print(f"  [TABLE {table_idx}] Getting table definition...")
                            sys.stdout.flush()
                            
                            table_def = conn.catalog.get_table_definition(table)
                            columns = [normalize(str(c.name)) for c in table_def.columns]
                            print(f"  [TABLE {table_idx}] Columns: {len(columns)} - {columns[:3]}...")
                            sys.stdout.flush()

                            # Execute query
                            query = f'SELECT * FROM "{schema_name}"."{raw_name}"'
                            print(f"  [TABLE {table_idx}] Executing: {query}")
                            sys.stdout.flush()
                            
                            rows = conn.execute_list_query(query)
                            print(f"  [TABLE {table_idx}] Rows fetched: {len(rows)}")
                            sys.stdout.flush()

                            # Create DataFrame
                            df = pd.DataFrame(rows, columns=columns)
                            print(f"  [TABLE {table_idx}] DataFrame created: {df.shape}")
                            sys.stdout.flush()

                            # Save to CSV
                            csv_name = f"{schema_name}_{table_name}.csv"
                            csv_path = os.path.join(output_dir, csv_name)
                            
                            print(f"  [TABLE {table_idx}] Saving to: {csv_path}")
                            sys.stdout.flush()
                            
                            df.to_csv(csv_path, index=False)
                            csv_files.append(csv_path)

                            table_info["exported"] = True
                            print(f"  [TABLE {table_idx}] ✓ SUCCESS")
                            sys.stdout.flush()

                        except Exception as table_error:
                            table_info["error"] = str(table_error)
                            print(f"  [TABLE {table_idx}] ✗ ERROR: {table_error}")
                            import traceback
                            traceback.print_exc()
                            sys.stdout.flush()

                        tables.append(table_info)

                print(f"\n[HYPER] Closing connection...")
                sys.stdout.flush()

            print("[HYPER] Connection closed ✓")
            sys.stdout.flush()

        print("[HYPER] HyperProcess terminated ✓")
        sys.stdout.flush()

    except Exception as hyper_error:
        print(f"\n[HYPER] ✗ CRITICAL ERROR: {hyper_error}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        raise

    print(f"\n{'='*60}")
    print(f"[HYPER END] Total CSV files: {len(csv_files)}")
    print(f"[HYPER END] Total tables: {len(tables)}")
    print(f"[HYPER END] Exported successfully: {sum(1 for t in tables if t.get('exported'))}")
    print(f"{'='*60}\n")
    sys.stdout.flush()

    return {
        "csv_files": csv_files,
        "tables": tables
    }
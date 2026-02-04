from tableauhyperapi import HyperProcess, Connection, Telemetry
import pandas as pd
import os

def normalize(name: str) -> str:
    return name.strip('"')

def extract_hyper_to_csv(hyper_path: str, output_dir: str, workbook_name: str):
    csv_files = []
    tables = []

    with HyperProcess(
        telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU
    ) as hyper:

        with Connection(endpoint=hyper.endpoint, database=hyper_path) as conn:

            for schema in conn.catalog.get_schema_names():
                schema_name = normalize(str(schema))

                for table in conn.catalog.get_table_names(schema):
                    table_name = normalize(str(table.name))

                    table_info = {
                        "schema": schema_name,
                        "table": table_name,
                        "exported": False
                    }

                    try:
                        table_def = conn.catalog.get_table_definition(table)
                        columns = [normalize(str(c.name)) for c in table_def.columns]

                        if schema_name.lower() == "extract":
                            rows = conn.execute_list_query(
                                f'SELECT * FROM "{schema_name}"."{table_name}"'
                            )

                            df = pd.DataFrame(rows, columns=columns)

                            # csv_name = f"{workbook_name}_{schema_name}_{table_name}.csv"
                            csv_name = f"{schema_name}_{table_name}.csv"
                            csv_path = os.path.join(output_dir, csv_name)

                            df.to_csv(csv_path, index=False)
                            csv_files.append(csv_path)

                            table_info["exported"] = True

                    except Exception as e:
                        table_info["error"] = str(e)

                    tables.append(table_info)

    return {
        "csv_files": csv_files,
        "tables": tables
    }

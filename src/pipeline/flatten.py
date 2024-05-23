import pandas as pd
import json
import psycopg2 as pg

TABLES_TO_CREATE = {
    "ceoPay",
    "fuel",
    "protectedAreas",
    "corruptionTraining",
    "passengerVehicles",
    "corporateDebtSecurities",
    "general",
    "renewableEnergyProduction",
    "electricityUsage",
    "staffCommuting",
    "emissionsToWater",
    "hotels",
    "ombudsman",
    "employeeTraining",
    "water",
    "sbti",
    "compensation",
    "refrigerantUsage",
    "highImpactClimateSectors",
    "nonRenewableEnergyConsumption",
    "revenue",
    "controversialWeapons",
    "naturalGas",
    "electricityUsage",
    "humanRightsRiskAssessment",
}


def get_query() -> str:
    subquery_for_type = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}' THEN '{table}'"
            for table in TABLES_TO_CREATE
        ]
    )

    subquery_for_extracted_data = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}'  THEN sections ->'{table}' -> 'table'"
            for table in TABLES_TO_CREATE
        ]
    )

    return f"""SELECT * , 
    sections ->'dates'->'date'->>'start' as start_date,
    sections ->'dates'->'date'->>'end'  AS end_date,
    sections -> 'siteId' as site_id ,
    CASE
            {subquery_for_extracted_data}
            ELSE NULL
    END AS extracted_data,
    CASE {subquery_for_type}
                ELSE NULL
            END AS type_of_data
    from submission_timelines ;
    """


def get_data_from_db() -> pd.DataFrame:
    # Define the PostgreSQL database connection details
    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"
    database = "postgres"

    engine = pg.connect(
        f"dbname='{database}' user='{username}' host='{host}' port='{port}' password='{password}'"
    )
    return pd.read_sql(sql=get_query(), con=engine)


def flatten_json(json_data):
    out = {}

    def flatten(x, name=""):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + "_")
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + "_")
                i += 1
        else:
            out[name[:-1]] = x

    flatten(json.loads(json_data))
    return out


# Sample data for demonstration
df_raw = get_data_from_db()
df_raw.insert(0, "synthetic_id", range(1, 1 + len(df_raw)))
df_raw.to_csv("raw_data.csv")

hashmap_of_df = {
    table: df_raw[df_raw["type_of_data"] == table] for table in TABLES_TO_CREATE
}

# print(hashmap_of_df)

#
for table, df in hashmap_of_df.items():
    # Concatenate the original DataFrame with the flattened JSON DataFrame
    df_normalized = pd.json_normalize(df["sections"])
    df_normalized["synthetic_id"] = df["synthetic_id"]

    print("dfn", df_normalized["synthetic_id"], "\ndf", df["synthetic_id"])

    #
    #     # Show the resulting DataFrame
    df_explodable_columns = df_normalized.applymap(type).eq(list).all()
    explodable_columns = df_normalized.columns[df_explodable_columns]

    for col in explodable_columns:
        df_exploded_column = df_normalized.explode(col)
        df_renormalized_column = pd.json_normalize(df_exploded_column[col])
        # df_new = pd.concat([df_normalized, df_renormalized_column], axis=1)
        # df_renormalized_column.to_csv("temp.csv")

    print("[df]", df.columns)
    print("[df_normalized]", df_normalized.columns)
    print("[df_renormalized_column]", df_renormalized_column.columns)
    # df['sections'] = df['sections'].apply(json.loads)

    # Use pd.json_normalize to flatten the 'sections' column
    df_normalized = pd.json_normalize(df["sections"])

    # Optional: Add 'id' column to df_normalized for joining purposes
    # df_normalized['id'] = df['id']

    # print(df_normalized['id'],df['id'] )

    # Step 3: Further normalize or explode the list of objects within 'df_normalized'
    # Assuming 'hotels.table' contains the list of dictionaries you want to normalize
    df_exploded = df_normalized.explode("hotels.table")

    # Normalize the exploded 'hotels.table' column
    df_renormalized_column = pd.json_normalize(df_exploded["hotels.table"])

    # Optional: Add 'id' column to df_renormalized_column for joining purposes
    # df_renormalized_column['id'] = df_exploded['id']

    # Step 4: Join the DataFrames together
    # First join df with df_normalized on 'id'
    df_combined = df.merge(df_normalized, on="id", how="left")

    # Then join the result with df_renormalized_column on 'id'
    # df_final = df_combined.merge(df_renormalized_column, on='id', how='left')

    # Drop any redundant columns if necessary
    # df_final = df_final.drop(columns=['hotels.table'])

    # Display the final DataFrame
    # print(df_combined)
    # df_combined.to_csv("data_cleaned_hotels.csv")

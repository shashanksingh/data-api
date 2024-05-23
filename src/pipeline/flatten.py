import pandas as pd
import json
import psycopg2 as pg

TABLES_TO_CREATE = (
    # "fuel",
    # "protectedAreas",
    # "corruptionTraining",
    # "passengerVehicles",
    # "corporateDebtSecurities",
    # "general",
    # "renewableEnergyProduction",
    # "electricityUsage",
    # "staffCommuting",
    # "emissionsToWater",
    "hotels",
    # "refrigerantUsage",
    # "highImpactClimateSectors",
    # "nonRenewableEnergyConsumption",
    # "revenue",
    # "controversialWeapons",
    # "naturalGas",
    # "electricityUsage",
    # "humanRightsRiskAssessment",
)

SUBQUERY = " ".join(
    [f"WHEN sections::jsonb ? '{table}' THEN '{table}'" for table in TABLES_TO_CREATE]
)

QUERY = f"""SELECT * , 
sections ->'dates'->'date'->>'start' as start_date,
sections ->'dates'->'date'->>'end'  AS end_date,
sections -> 'siteId' as site_id ,
CASE {SUBQUERY}
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
    return pd.read_sql(sql=QUERY, con=engine)


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

hashmap_of_df = {
    table: df_raw[df_raw["type_of_data"] == table] for table in TABLES_TO_CREATE
}

# print(hashmap_of_df)

#
for table, df in hashmap_of_df.items():
    # Concatenate the original DataFrame with the flattened JSON DataFrame
    df_normalized = pd.json_normalize(df["sections"])

    # Show the resulting DataFrame
    df_explodable_columns = df_normalized.applymap(type).eq(list).all()
    explodable_columns = df_normalized.columns[df_explodable_columns]

    for col in explodable_columns:
        df_normalized = df_normalized.explode(col)

    for col in explodable_columns:
        if col in df.columns:
            df_normalized = pd.json_normalize(df[col])
            df = pd.concat([df.drop(columns=[col]), df_normalized], axis=1)

    print(df)

    df.to_csv(f"data_cleaned_{table}.csv")


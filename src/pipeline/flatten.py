import pandas as pd
import json
import psycopg2 as pg

TABLES_TO_CREATE = (
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
    "refrigerantUsage",
    "highImpactClimateSectors",
    "nonRenewableEnergyConsumption",
    "revenue",
    "controversialWeapons",
    "naturalGas",
    "electricityUsage",
    "humanRightsRiskAssessment",
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
from submission_timelines order by updated_at  desc limit 1;
"""

print(QUERY)


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
    df = pd.read_sql(sql=QUERY, con=engine)
    return df


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

print(hashmap_of_df)

#
# for table, df in hashmap_of_df.items():
#     # Apply the function to flatten the JSON column
#     json_cols = df["sections"].apply(flatten_json)
#
#     # Convert the list of dictionaries into a DataFrame
#     json_df = pd.json_normalize(json_cols)
#
#     # Concatenate the original DataFrame with the flattened JSON DataFrame
#     df = pd.concat([df.drop(columns=["sections"]), json_df], axis=1)
#
#     # Show the resulting DataFrame
#     print(table)
#
#     df.to_csv(f"data_flatten_{table}.csv")

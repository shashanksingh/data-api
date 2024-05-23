import pandas as pd
import json
import psycopg2 as pg

TABLES_TO_CREATE = {
    # "ceoPay",
    # "employeeData",
    # "employeeTraining",
    # "employeeTurnover",
    # "financial",
    # "governanceStandards",
    # "humanRightsAssessment",
    # "humanRightsRisks",
    # "passengerVehicles",
    # "electricVehicles",
    # "heavyVehicles",
    # "waterUsage",
    # "refridgerantUsage",
    # "energyUsage",
    # "sites",
    # "minimumWageGuarantee",
    # "BOD",
    # "injuryRate",
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
    # "ombudsman",
    # "employeeTraining",
    # "water",
    # "sbti",
    # "compensation",
    # "refrigerantUsage",
    # "highImpactClimateSectors",
    # "nonRenewableEnergyConsumption",
    # "revenue",
    # "controversialWeapons",
    # "naturalGas",
    # "electricityUsage",
    # "humanRightsRiskAssessment",
    # "direct",
    # "employeeHiring",
    # "whistleblower",
    # "scopeThreeEmissions",
    # "minimumWageGuaranteeSchema",
    # "corruptionIncidents",
    # "fuelUsage",
    # "ESGriskAndOpportunity",
    # "purposeStatement",
    # "daysLostInjuries",
    # "consulatationProcess",
    # "averageBased",
    # "incomeStatements",
    # "realEstateEnergyEfficiency",
    # "catsAndDogs",
    # "fossilFuelSectorActivity",
    # "stakeholderEngagement",
    # "biodiversitySensitiveAreas",
    # "hazardousWaste",
    # "OECDGuidelinesMultinational",
    # "UNGCPrinciples",
    # "realEstateFossilFuels",
    # "cSuiteDiversity",
    # "bod",
    # "renewableEnergy",
    # "airPollutants",
    # "fuel",

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
            f"WHEN sections::jsonb ? '{table}'  THEN sections ->'{table}' "
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
df_raw.dropna(subset=['type_of_data'], inplace=True)
# df_raw.to_csv("raw_data.csv")

hashmap_of_df = {
    table: df_raw[df_raw["type_of_data"] == table] for table in TABLES_TO_CREATE
}

# print(hashmap_of_df)

#
for table, df in hashmap_of_df.items():
    # Concatenate the original DataFrame with the flattened JSON DataFrame
    df_exploded = df.explode('extracted_data')
    df_normalized = pd.json_normalize(df['extracted_data'], "table")

    df_normalized.to_csv("raw_data.csv")


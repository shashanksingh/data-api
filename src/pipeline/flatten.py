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
    "injuryRate",
    "fuel",
    # "protectedAreas",
    # "corruptionTraining",
    # "passengerVehicles",
    # "corporateDebtSecurities",
    # "general",
    # "renewableEnergyProduction",
    # "electricityUsage",
    # "staffCommuting",
    # "emissionsToWater",
    # "hotels",
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
    "naturalGas",
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
            f"WHEN extracted_data_with_id::jsonb ? '{table}'  THEN extracted_data_with_id ->'{table}' "
            for table in TABLES_TO_CREATE
        ]
    )

    subquery_for_extracted_data_with_id = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}'  THEN Jsonb_insert( sections::jsonb , '{{ {table}, primary_key}}', To_jsonb(id), true ) "
            for table in TABLES_TO_CREATE
        ]
    )

    return f"""
    WITH extracted_data_cte AS (
        SELECT
            *,
            id as primary_key,
            sections -> 'dates' -> 'date' ->> 'start' AS start_date,
            sections -> 'dates' -> 'date' ->> 'end' AS end_date,
            sections -> 'siteId' AS site_id,
            CASE {subquery_for_type}
                ELSE NULL
            END AS type_of_data,
            CASE
               {subquery_for_extracted_data_with_id}
                ELSE NULL
            END AS extracted_data_with_id
        FROM
            submission_timelines
    )
    SELECT
        *,
        CASE
            {subquery_for_extracted_data}
                ELSE NULL
            END AS extracted_data
    FROM
        extracted_data_cte
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
    print(get_query())
    return pd.read_sql(sql=get_query(), con=engine)


df_raw = get_data_from_db()
df_raw.insert(0, "synthetic_id", range(1, 1 + len(df_raw)))
df_raw.dropna(subset=["type_of_data"], inplace=True)
# df_raw.to_csv("raw_data.csv")

hashmap_of_df = {
    table: df_raw[df_raw["type_of_data"] == table] for table in TABLES_TO_CREATE
}


for table, df in hashmap_of_df.items():

    df_normalized = pd.json_normalize(
        df["extracted_data"], record_path="table", meta=["primary_key"]
    )

    df_final = pd.merge(df, df_normalized, on="primary_key", how="left")
    df_final.to_csv(f"data_normazlied_{table}.csv")

from typing import Any, Callable

import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine
import functools


DIMENSIONS = {
    # "public.countries": "dimension_public_  countries",
    # "public.sites": "dimension_public_sites",
    # "conversion_factors.fuel": "dimension_conversion_factor_fuel",
    "conversion_factors.metadata": "dimension_conversion_factor_metadata",
    # "conversion_factors.rate_sources": "dimension_conversion_factor_rate_sources",
}

FACTS = {
    # "ceoPay",
    # "employeeData",
    # "employeeTraining",
    # "employeeTurnover",
    # "financial",
    # "governanceStandards",
    # "humanRightsAssessment",
    # "humanRightsRisks",
    # "passengerVehicles",
    "electricVehicles",
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


def get_submission_timeline_query() -> str:
    subquery_for_type = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}' THEN '{table}'"
            for table in FACTS
        ]
    )

    subquery_for_extracted_data = " ".join(
        [
            f"WHEN extracted_data_with_id::jsonb ? '{table}'  THEN extracted_data_with_id ->'{table}' "
            for table in FACTS
        ]
    )

    subquery_for_extracted_data_with_id = " ".join(
        [
            f"WHEN sections::jsonb ? '{table}'  THEN Jsonb_insert( sections::jsonb , '{{ {table}, primary_key}}', "
            f"To_jsonb(id), true )"
            for table in FACTS
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


def get_fullload_query(table: str) -> str:
    return f"SELECT * from {table}"


def get_engine(database: str = "postgres") -> Any:
    # Define the PostgreSQL database connection details
    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"

    return pg.connect(
        f"dbname='{database}' user='{username}' host='{host}' port='{port}' password='{password}'"
    )


def get_data_from_db(sql_callback: Callable) -> pd.DataFrame:
    # print(get_submission_timeline_query())
    return pd.read_sql(sql=sql_callback(), con=get_engine())


# submission timeline
df_raw = get_data_from_db(sql_callback=get_submission_timeline_query)

# temporary
df_raw.dropna(subset=["type_of_data"], inplace=True)

hashmap_of_df = {
    table: df_raw[df_raw["type_of_data"] == table] for table in FACTS
}

for table, df in hashmap_of_df.items():
    df_normalized = pd.json_normalize(
        df["extracted_data"], record_path="table", meta=["primary_key"]
    )

    df_final = pd.merge(df, df_normalized, on="primary_key", how="left")

    username = "admin"
    password = "supersecret123"
    host = "localhost"
    port = "5432"

    engine = create_engine(
        f"postgresql+psycopg2://{username}:{password}@{host}/reporting"
    )

    df_final.drop(
        ["sections", "extracted_data_with_id", "extracted_data"], axis=1
    ).to_sql(name=table, con=engine, if_exists="append", method="multi")

# Dimension
for table, name in DIMENSIONS.items():
    partial_callback = functools.partial(get_fullload_query, table=table)

    df_raw = get_data_from_db(sql_callback=partial_callback)
    nested_cols = df_raw.dtypes


    print(nested_cols)
    # df_raw.to_sql(name=name, con=engine, if_exists="append", method="multi")

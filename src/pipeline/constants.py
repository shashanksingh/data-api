from dataclasses import dataclass


@dataclass
class Unit:
    name: str
    label: str
    convertTo: str = None
    conversion: str = None


hashmap_of_units = {
    "d11e91ed-3fb3-40af-91a5-514e02103866": Unit(name="KWH_UNIT", label="kWh"),
    "60799459-2fd4-4998-8b10-59066b386102": Unit(
        name="MWH_UNIT",
        label="MWh",
        convertTo="KWH_UNIT",
        conversion="increaseValue1000",
    ),
    "477522a8-76fd-49da-a0b6-9867bdc39cac": Unit(name="LITRES_UNIT", label="L"),
    "KILOLITRES_UNIT": Unit(
        id="c0a76f6c-86a3-4333-b45c-6cf04cccddf4",
        label="kL",
        convertTo="LITRES_UNIT",
        conversion="increaseValue1000",
    ),
    "CUBIC_METERS_UNIT": Unit(
        id="bc0e42d7-0dfe-494b-b8ea-41990f2c821c", label="Cubic meters"
    ),
    "GALLONS_UNIT": Unit(
        id="6d7a7901-ce71-49d7-b3d0-38036b69635e",
        label="US Liquid Gallons",
        convertTo="LITRES_UNIT",
        conversion="usGallonsToLitres",
    ),
    "KG_UNIT": Unit(id="7abe5fe6-f713-44c4-8f91-aa76061ed4ee", label="Kg"),
    "TONS_UNIT": Unit(id="7ab7abca-a8f4-46be-b8f3-d408bd54d0b1", label="Tons"),
    "TONNES_UNIT": Unit(id="cd58b882-5f78-4f5e-8d7b-ae14954db3eb", label="Tonnes"),
    "TONNES_KM_UNIT": Unit(
        id="301f1bf6-915b-4cca-8dbc-0225aae5d64f", label="Tonnes-km"
    ),
    "KM_UNIT": Unit(id="c4bd40f7-c609-420a-bb15-2ce7e8aedc2b", label="Km"),
    "PASSENGER_KM_UNIT": Unit(id="c009489c-89ab-4b04-831c-605f9c6daa7f", label="Km"),
    "MILES_UNIT": Unit(
        id="5275ea32-58d0-42b4-afc7-8f7f5f423dd7",
        label="Mi",
        convertTo="KM_UNIT",
        conversion="milesToKm",
    ),
    "PASSENGER_MILES_UNIT": Unit(
        id="42c7cd4f-1ce4-479b-b9c9-009f3d9317c2",
        label="Mi",
        convertTo="PASSENGER_KM_UNIT",
        conversion="milesToKm",
    ),
    "MEGALITRES_UNIT": Unit(id="da30e86d-1afa-4b73-bf94-580c64bb4acf", label="ML"),
    "GWH_UNIT": Unit(id="d7d384f5-0152-436c-90c7-e8f696ee4baa", label="GWh"),
    "GBP_UNIT": Unit(id="bb265c1a-23ce-4761-9a5b-47491feecb2d", label="GBP"),
    "USD_UNIT": Unit(id="ffa91f39-ff16-4eb2-bea5-e8875aec61d0", label="USD"),
    "KWH_NET": Unit(id="accdfdb5-9db6-41f3-a990-c426434255fc", label="KWh - net"),
}

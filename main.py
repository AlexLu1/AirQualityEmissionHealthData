import psycopg2
from sqlDataParser import DataParser
from OWID.OWIDDatasetM import OWIDDataset
from WHO_HFA.WHODataM import WHOData
from EDGAR_Emissions.EDGARDatasetM import EDGARData
from AirQuality.airQualityM import AirQualityData
import os


# Database connection parameters
db_params = {
    "dbname": "exampleDatabase",
    "user": "postgres",
    "password": "superuser",
    "host": "localhost",
    "port": 5432
}

#Database Schema
create_schema = """
CREATE TABLE "country" (
    "countryCode" VARCHAR(3) PRIMARY KEY,
    "name" VARCHAR(255)
);

CREATE TABLE "countryInfo" (
    "year" INT NOT NULL,
    "countryCode" VARCHAR(3) NOT NULL,
    "population" INT,
    "gdp" INT,
    "energyConsumptionPerCapita" FLOAT,
    PRIMARY KEY ("year", "countryCode"),
    FOREIGN KEY ("countryCode") REFERENCES "country"("countryCode")
);

CREATE TABLE "city" (
    "city_ID" SERIAL PRIMARY KEY,
    "name" VARCHAR(255),
    "countryCode" VARCHAR(3),
    FOREIGN KEY ("countryCode") REFERENCES "country"("countryCode")
);

CREATE TABLE "sector" (
    "sectorCode" VARCHAR(50) PRIMARY KEY,
    "name" TEXT
);

CREATE TABLE "measureUnit" (
    "measureUnitCode" VARCHAR(50) PRIMARY KEY,
    "name" VARCHAR(100),
    "description" TEXT
);

CREATE TABLE "chemical" (
    "chemicalCode" VARCHAR(50) PRIMARY KEY,
    "name" VARCHAR(255)
);

CREATE TABLE "emissionData" (
    "emissionData_ID" SERIAL PRIMARY KEY,
    "year" INT NOT NULL,
    "value" FLOAT NOT NULL,
    "fossil_bio" VARCHAR(10),
    "countryCode" VARCHAR(3) REFERENCES "country"("countryCode"),
    "sectorCode" VARCHAR(50) REFERENCES "sector"("sectorCode"),
    "chemicalCode" VARCHAR(50) REFERENCES "chemical"("chemicalCode"),
    "measureUnitCode" VARCHAR(50) REFERENCES "measureUnit"("measureUnitCode")
);

CREATE TABLE "sDRRespiratoryDisease" (
    "rate" FLOAT NOT NULL,
    "year" INT NOT NULL,
    "countryCode" VARCHAR(10) REFERENCES Country("countryCode"),
    PRIMARY KEY ("year", "countryCode")
);

CREATE TABLE "airMeasurement" (
    "airMeasurement_ID" BIGSERIAL PRIMARY KEY,
    "date" DATE NOT NULL,
    "value" FLOAT NOT NULL,
    "city_ID" INT REFERENCES city("city_ID"),
    "measureUnitCode" VARCHAR(50) REFERENCES "measureUnit"("measureUnitCode"),
    "chemicalCode" VARCHAR(50) REFERENCES "chemical"("chemicalCode")
);
"""
#Connect to database and execute schema query
def create_database_schema():
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_schema)
    except psycopg2.Error as e:
        print("An error occurred while creating the schema:", e)
    finally:
        if conn:
            conn.close()
            
#Connect to database and execute schema query
def main():
    sqlDataParser = DataParser(db_params["user"],db_params["password"],db_params["host"],db_params["port"],db_params["dbname"])
    #Create Schema
    print("Parsing Schema.")
    create_database_schema()
    print("Schema Created successfully.")
    #Parse OWID Dataset Data
    print("Parsing OWID Data")
    owidDataset = OWIDDataset(sqlDataParser)
    owidDataset.parseCountries()
    owidDataset.parseCountryInfomation()
    print("OWID Data parsed successfully.")
    #Parse Health Dataset
    print("Parsing HFA Data")
    wHODataset = WHOData(sqlDataParser)
    wHODataset.parseWHOData()
    print("HFA Data parsed successfully.")
    #Parse EDGAR Emission data
    print("Parsing EDGAR Emission Data")
    edgarData = EDGARData(sqlDataParser)
    edgarData.parseEdgarData()
    print("EDGAR Emission Data parsed sucessfully")
    #Parse Air Quality Dataset
    print("Parsing Air Quality Data")
    airQualityDataset = AirQualityData(sqlDataParser,db_params)
    airQualityDataset.parseAllAirQualityData(True,False,False)
    print("Air Quality Data Parsed successfully")


# Main entry point
if __name__ == "__main__":
    main()
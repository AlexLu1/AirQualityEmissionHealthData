import pandas as pd
from sqlDataParser import DataParser
import os

class OWIDDataset:
    def __init__(self,dataParser:DataParser):
        self.dataParser = dataParser

    def parseCountries(self):
        owidData = pd.read_csv(os.path.join("OWID","owid-co2-data.csv"))
        #keep relevant country data, remove duplicate rows and null value iso_codes
        country = owidData[["iso_code","country"]].drop_duplicates().dropna(subset=["iso_code"])
        #rename columns according to sql database
        country = country.rename(columns={"iso_code": "countryCode", "country": "name"})
        self.dataParser.parsePandaDFToTable(country,"country")

    def parseCountryInfomation(self):
        owidData = pd.read_csv(os.path.join("OWID","owid-co2-data.csv"))
        #tranform to desired form
        owidData = owidData[["year","iso_code","population","gdp","energy_per_capita"]]
        owidData = owidData.dropna(subset=["iso_code"])
        owidData = owidData.rename(columns={"iso_code": "countryCode", "energy_per_capita": "energyConsumptionPerCapita"})
        #fill nan values with -1
        owidData["population"] = owidData["population"].fillna(-1).astype(int)
        owidData["gdp"] = owidData["gdp"].fillna(-1).astype(int)
        self.dataParser.parsePandaDFToTable(owidData,"countryInfo")


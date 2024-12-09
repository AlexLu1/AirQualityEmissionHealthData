import pandas as pd
from sqlDataParser import DataParser
import os

class WHOData:
    def __init__(self,dataParser:DataParser):
        self.dataParser = dataParser

    def parseWHOData(self):
        column_names = ["countryCode", "COUNTRY_GRP", "SEX", "year", "rate"]
        who_data = pd.read_csv(os.path.join("WHO_HFA","HFA_221_EN.csv"), skiprows=26, names=column_names, header=None)
        who_data = who_data[["countryCode","year","rate"]]
        who_data = who_data.dropna(subset=["year", "rate", "countryCode"])
        who_data["year"] = who_data["year"].astype(int)
        self.dataParser.parsePandaDFToTable(who_data,"sDRRespiratoryDisease")



import pandas as pd
from sqlDataParser import DataParser
import os


class EDGARData():
    """
    Class Responsible for parsing EDGAR data.
    """
    
    def __init__(self,dataParser:DataParser):
        self.dataParser = dataParser

    def parseEdgarData(self):
        #Emissions Database for Global Atmospheric Research Dataset, Greenhouse Gas information comes in the form of multiple xlsx files
        EDGAREmissionDataList = ["IEA_EDGAR_CO2_1970_2023.xlsx","EDGAR_N2O_1970_2023.xlsx","EDGAR_F-gases_1990_2023.xlsx","EDGAR_CO2bio_1970_2023.xlsx","EDGAR_CH4_1970_2023.xlsx","EDGAR_AR5g_F-gases_1990_2023.xlsx","EDGAR_AR5_GHG_1970_2023.xlsx"]
        #These are the tables we are interested in creating from this dataset
        emissionDataAll =  pd.DataFrame()
        sectorAll = pd.DataFrame()
        chemicalAll = pd.DataFrame()
        #measureUnit for all datasets is the same 
        measureUnit = pd.DataFrame({'measureUnitCode': ['Gg'], 'name': ['Gigagrams']})
        #they have very similiar structure so we can use the same procedure to build our dataframes
        for data in EDGAREmissionDataList:
            #read excel file
            edgarData = pd.read_excel(os.path.join("EDGAR_Emissions","data",data), sheet_name='IPCC 2006',header=9)
            #sector data
            sector = edgarData[["ipcc_code_2006_for_standard_report","ipcc_code_2006_for_standard_report_name"]].drop_duplicates()
            sector = sector.rename(columns={"ipcc_code_2006_for_standard_report": "sectorCode", "ipcc_code_2006_for_standard_report_name": "name"})
            #chemical data
            chemical = edgarData[["Substance"]].drop_duplicates().rename(columns={"Substance": "chemicalCode"})
            chemical['name'] = pd.NA
            #emission data
            #transform individual year columns into one year and value column
            emissionData = pd.melt(frame = edgarData, 
                                id_vars=['IPCC_annex','Country_code_A3','ipcc_code_2006_for_standard_report','Substance','fossil_bio'], 
                                var_name='year', 
                                value_vars=edgarData.columns[9:],
                                value_name='value'
                            )
            #bring data to form of sql database
            emissionData = emissionData.drop('IPCC_annex',axis = 1)
            emissionData['year'] = emissionData['year'].str.replace('Y_', '').astype(int)
            emissionData['value'] = emissionData['value'].astype(float)
            emissionData = emissionData.rename(columns={"Country_code_A3": "countryCode","ipcc_code_2006_for_standard_report":"sectorCode","Substance":"chemicalCode"})
            #Append to rest of data
            emissionDataAll = pd.concat([emissionDataAll,emissionData],ignore_index=True)
            sectorAll = pd.concat([sectorAll,sector],ignore_index=True)
            chemicalAll = pd.concat([chemicalAll,chemical],ignore_index=True)

        #Add measure unit
        emissionDataAll["measureUnitCode"] = "Gg"
        #drop potential duplicate data from concatenation
        emissionDataAll = emissionDataAll.drop_duplicates()
        sectorAll = sectorAll.drop_duplicates()
        chemicalAll = chemicalAll.drop_duplicates()
        #Drop null values
        emissionDataAll = emissionDataAll.dropna(subset=["value"])
        #Drop invalid country codes
        queryCodes = self.dataParser.makeCall("""SELECT DISTINCT "countryCode" FROM country""")
        countryCodes = [item[0] for item in queryCodes]
        emissionDataAll = emissionDataAll[emissionDataAll['countryCode'].isin(countryCodes)]
        self.dataParser.parsePandaDFToTable(chemicalAll,"chemical")
        self.dataParser.parsePandaDFToTable(measureUnit,"measureUnit")
        self.dataParser.parsePandaDFToTable(sectorAll,"sector")
        self.dataParser.parsePandaDFToTable(emissionDataAll,"emissionData")

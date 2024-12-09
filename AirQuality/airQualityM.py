from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os
from sqlDataParser import DataParser

class AirQualityData():
    """
    Class Responsible for parsing Air Quality data.
    """
    def __init__(self,dataParser:DataParser):
        #We need a pollutant mapping from type to notation
        pollutant = pd.read_csv(os.path.join("AirQuality","chemicalVocabulary.csv"))
        pollutant = pollutant[["chemicalID","chemicalCode"]]
        pollutant["chemicalID"] = pollutant["chemicalID"].astype(int)
        self.pollutantMapNotation = pollutant.set_index("chemicalID")["chemicalCode"].to_dict() 
        #We need a pollutant mapping from type to recommended unit notation for empty unit data
        chemUnitMap = pd.read_csv(os.path.join("AirQuality","chemicalUnitMap.csv"))
        self.pollutantMapUnit = chemUnitMap.set_index("chemicalID")["recommendedUnit"].to_dict()
        #we need a dictonary to convert from iso2 to iso3 country codes      
        countryCodes = pd.read_csv(os.path.join("AirQuality","countryCodes.csv"))
        countryCodes = countryCodes[["alpha-2","alpha-3"]]
        self.countryCodeMap = countryCodes.set_index("alpha-2")["alpha-3"].to_dict()
        self.dataParser = dataParser

    def parseAllAirQualityData(self,dataset1:bool,dataset2:bool,dataset3:bool):
        """
        Parses all relevant data from the EAA air quality dataset
        Warning, dataset 3 has the size of 800 GB.
        For testing just loading dataset 1 and 2 is recommended at a size of 4GB.

        :param p1: parse air dataset1
        :param p2: parse air dataset2
        :param p3: parse air dataset3
        """ 
        self.parseCityData()
        print("City data parsed successfully.")
        self.parsePollutantData()
        print("Chemical data parsed successfully.")
        self.parseMeasurementData()
        print("Measurement data parsed successfully.")
        #download parquet files urls
        self.downloadParquetUrls(dataset1,dataset2,dataset3)
        print("Parquet file urls downloaded successfully.")
        #download data
        self.download_parquet_files(os.path.join("AirQuality","download"))
        self.parseAirQualityData(os.path.join("AirQuality","download"))
    
    def downloadParquetUrls(self,dataset1,dataset2,dataset3):
        countryCity = self.fetchCountryCityData()
        for _, row in countryCity.iterrows():
            country_code = row['countryCode']
            city_name = row['cityName']
            if(dataset1):
                self.make_ParquetRequest(country_code, city_name,1,"day")
            if(dataset2):
                self.make_ParquetRequest(country_code, city_name,2,"day")
            if(dataset3):
                self.make_ParquetRequest(country_code, city_name,3,"day")

    def fetchCountryCityData(self):
        #Api URL
        EAAAPi = "https://eeadmz1-downloads-api-appservice.azurewebsites.net"
        #fetch all available countries 
        countriesResponse = requests.get(F"{EAAAPi}/Country")
        if countriesResponse.status_code != 200:
            raise Exception(f"Failed to fetch countries: {countriesResponse.status_code}")
        countries = countriesResponse.json()

        countryCityData = []
        #fetch all available cities for each country
        for country in countries:
            country_code = country["countryCode"]
            country_name = country["countryName"]
            #Send request for country
            citiesResponse = requests.post(F"{EAAAPi}/City", json=[country_code])
            if citiesResponse.status_code != 200:
                print(f"Failed to fetch cities for {country_name} ({country_code}): {citiesResponse.status_code}")
                continue
            cities = citiesResponse.json()
            #store result in dictionary
            for city in cities:
                countryCityData.append({
                    "countryCode": country_code,
                    "countryName": country_name,
                    "cityName": city["cityName"]
                })

        #create DataFrame
        return pd.DataFrame(countryCityData)

    def make_ParquetRequest(self,country_code, city_name,dataset,aggregationType):
        #create folder structure for download
        sanitized_city_name = city_name.replace(" ", "_").replace("/", "_").replace("\\", "_")
        folder_path = os.path.join("AirQuality","download", country_code, sanitized_city_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        #make parquet file request to EEA API
        request= {
            "countries": [country_code],
            "cities": [city_name],
            "pollutants": [],
            "dataset": dataset,
            "aggregationType": aggregationType
        }
        response = requests.post("https://eeadmz1-downloads-api-appservice.azurewebsites.net/ParquetFile/urls", json=request)
        if response.status_code != 200:
            print(f"failed to download file for {country_code} - {city_name}: {response.status_code}")
            return
        #check response
        try:
            #fetch hourly entries instead if no daily entries exist
            if(len(response.content) <= 16):
                if(dataset != 3 and aggregationType == "day" ):
                    self.make_ParquetRequest(country_code, city_name,dataset,"hour")
                #print(f"No daily entries for: {country_code} {city_name}")
                return
            #write binary file
            file_name = f"urlFiles{dataset}.csv"
            urlFilePath = os.path.join(folder_path, file_name)
            with open(urlFilePath, 'wb') as output:
                output.write(response.content)
            #write txt
            info_file = os.path.join(folder_path, "info.txt")
            with open(info_file, 'w') as output:
                output.write(city_name+"\n"+ country_code)
        except Exception as e:
            print(f"error writing file for {country_code} - {city_name}: {e}")

    #downloads all parquet files into folder structure, given a csv with links
    def download_parquet_files(self,root_folder: str):
        for dirpath, _, filenames in os.walk(root_folder):
            #get all csv download files
            csv_files = [f for f in filenames if f.startswith('urlFiles') and f.endswith('.csv')]
            if(len(csv_files)<=0):
                continue
            downloads = pd.DataFrame()
            for csv_file in csv_files:
                csv_path = os.path.join(dirpath, csv_file)
                df = pd.read_csv(csv_path)
                downloads = pd.concat([downloads, df], ignore_index=True)
            print(f"Downloading from {dirpath}")
            self.download_files_from_dataframe(downloads,dirpath)

    def download_file(self,url,output_folder):
        filename = f"{url.split('/')[-3]}_{os.path.basename(url.split('?')[0])}" 
        output_path = os.path.join(output_folder, filename)
        if os.path.exists(output_path):
            print(f"File already exists {output_path}")
            return
        try:
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            return f"Failed to download {url}. Error: {e}"
        
    def download_files_from_dataframe(self,df, output_folder, max_workers=40):
        os.makedirs(output_folder, exist_ok=True)
        urls = df["ParquetFileUrl"].dropna().unique()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for url in urls:
                executor.submit(self.download_file, url,output_folder)

    def parseAirQualityData(self,root_folder,max_workers=5):
        for dirpath, _, filenames in os.walk(root_folder):
        #get all csv download files
            parquetFiles = [f for f in filenames if f.endswith('.parquet')]
            if(len(parquetFiles)<1):
                continue
            #read info file
            infoFileP = os.path.join(dirpath, "info.txt")
            try:
                with open(infoFileP,'r') as infoFile:
                    cityName = infoFile.readline().strip() 
                    countryCodeIso2 = infoFile.readline().strip()
            except Exception as e:
                print(f"Couldnt read {infoFileP}.")
                continue
            futures = []
            print(f"Parsing files from: {dirpath}")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for parquetFile in parquetFiles:
                    future = executor.submit(self.parseParquetFile, os.path.join(dirpath, parquetFile), cityName, countryCodeIso2)
                    futures.append(future)
            for future in as_completed(futures):
                try:
                    future.result() 
                except Exception as e:
                    print(f"Caught exception while parsing air data: {e}")



    def parseParquetFile(self,parquetFilePath,cityName,countryCodeIso2):
        airMeasurment = pd.read_parquet(parquetFilePath)
        #filter out invalid rows
        airMeasurment = airMeasurment[airMeasurment['Validity'] != -1]
        if len(airMeasurment) == 0:
            return
        airMeasurment = airMeasurment[["Pollutant","Start","Value","Unit","AggType"]]
        if ((airMeasurment['AggType'] == 'hour').any()):
            airMeasurment = airMeasurment.groupby(pd.Grouper(key='Start', freq='D')).agg({
                'Pollutant': 'first',  
                'Value': 'mean',      
                'Unit': 'first',      
                'AggType': 'first'    
            }).reset_index()
        #grouping can create additional values
        airMeasurment = airMeasurment.dropna(subset=["Pollutant", "Value"])
        airMeasurment = airMeasurment[["Pollutant","Start","Value","Unit"]]
        airMeasurment["Pollutant"] = airMeasurment["Pollutant"].astype(int)
        airMeasurment["Value"] = airMeasurment["Value"].astype(float)
        #get recommendedUnit in case unit in dataset is empty (happens a lot)
        recommendedUnit = self.pollutantMapUnit.get(airMeasurment['Pollutant'].iloc[0],"noUnitFound")
        airMeasurment["Pollutant"] = self.pollutantMapNotation.get(airMeasurment["Pollutant"].iloc[0],"noChemicalFound")
        airMeasurment = airMeasurment.rename(columns={"Pollutant": "chemicalCode","Start":"date","Value":"value","Unit":"measureUnitCode"})
        #fill unit if necessary
        airMeasurment['measureUnitCode'] = airMeasurment['measureUnitCode'].fillna(recommendedUnit)
        #Sanitize more
        airMeasurment = airMeasurment[airMeasurment['measureUnitCode'] != "noUnitFound"]
        airMeasurment = airMeasurment[airMeasurment['chemicalCode'] != "noChemicalFound"]
        airMeasurment = airMeasurment[airMeasurment['value'] >= 0]
        if len(airMeasurment) == 0:
            return
        countryCode = self.countryCodeMap.get(countryCodeIso2)
        query = """
            SELECT "city_ID"
            FROM city
            WHERE "name" = :city_name AND "countryCode" = :country_code
        """
        params = {
            "city_name": cityName,  
            "country_code": countryCode               
        }
        query = self.dataParser.makeCall(query,params)
        result = [item[0] for item in query]
        if(len(result) < 1):
            print(f"No city id for {cityName} {countryCode} found.")
            return
        airMeasurment["city_ID"] = int(result[0])
        self.dataParser.parsePandaDFToTable(airMeasurment,"airMeasurement")

    def parsePollutantData(self):
        dfChemical = pd.read_csv(os.path.join("AirQuality","chemicalVocabulary.csv"))
        dfChemical = dfChemical[["chemicalCode","name"]]
        #remove duplicates
        dfChemical = dfChemical.drop_duplicates(subset=['chemicalCode'])
        #remove already existing chemicals
        query = self.dataParser.makeCall("""SELECT DISTINCT "chemicalCode" FROM chemical""")
        chemicals = [item[0] for item in query]
        dfChemical = dfChemical[~dfChemical['chemicalCode'].isin(chemicals)]
        #parse
        self.dataParser.parsePandaDFToTable(dfChemical,"chemical")

    def parseMeasurementData(self):
        measurementData = pd.read_csv(os.path.join("AirQuality","concentration.csv"))
        measurementData = measurementData[["URI","Label","Definition"]]
        measurementData["URI"] = measurementData["URI"].apply(lambda x : os.path.basename(x))
        measurementData = measurementData.rename(columns={"URI": "measureUnitCode","Label":"name","Definition":"description"})
        self.dataParser.parsePandaDFToTable(measurementData,"measureUnit")  


    def parseCityData(self):
        #fetch our data from the api
        countryCityData= self.fetchCountryCityData()
        #we need to convert from iso2 to iso3 country codes, we will use another mapping for this
        countryCodes = pd.read_csv(os.path.join("AirQuality","countryCodes.csv"))
        countryCodes = countryCodes[["alpha-2","alpha-3"]]
        #merge tables on 3 digit country code
        countryCityData = countryCityData.merge(countryCodes, left_on="countryCode", right_on="alpha-2", how="left")
        #change structure to sql table
        city = countryCityData[["alpha-3","cityName"]].rename(columns={"alpha-3": "countryCode","cityName":"name"})
        #parse data to database
        self.dataParser.parsePandaDFToTable(city,"city")        




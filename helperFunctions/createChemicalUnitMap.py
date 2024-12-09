import pandas as pd
import os
from bs4 import BeautifulSoup
import csv

def fetchMandatoryUnit(pollutant_id):
    url = f"https://dd.eionet.europa.eu/vocabularyconcept/aq/pollutant/{pollutant_id}" 
    #fetch the webpage
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch webpage. Status code: {response.status_code}")
    #parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", class_="datatable results")
    if not table:
        return "nothing"
    #find the relevant header (Mandatory or Recommended unit)
    unitRow = None
    for th in table.find_all("th"):
        if "Mandatory unit" in th.text.strip():
            unitRow = th
            break
        if "Recommended unit" in th.text.strip():
            unitRow = th
    #extract the hyperlink from the corresponding cell
    if unitRow:
        unit_cell = unitRow.find_next_sibling("td")
        if unit_cell:
            #find the first hyperlink within the cell
            link = unit_cell.find("a")
            if link and link.has_attr("href"):
                return link["href"]
            
    return "nothing"

df = pd.read_csv("chemicalVocabulary.csv")
data = []
for code in df["chemicalID"]:
    unit = fetchMandatoryUnit(code)
    if unit.endswith("/view"):
        unit = unit[:-5]
    unit = os.path.basename(unit)
    data.append([code,unit])
    headers = ["chemicalID","recommendedUnit"]
    
with open("chemicalUnitMap.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(data)
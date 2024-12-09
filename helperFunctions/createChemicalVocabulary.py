from bs4 import BeautifulSoup
import csv

with open("Data Dictionary - Vocabulary.htm", "r") as file:
    html_content = file.read()

# Parse the HTML content
soup = BeautifulSoup(html_content, "html.parser")
# Find the table
table = soup.find("table",class_="datatable results",id = "concept")  
rows = table.find_all("tr")
# Loop through the rows and extract the data
data = []
for row in rows:
    columns = row.find_all("td")  # Find all <td> elements (table data cells)
    if columns:
        # Extract text from each cell
        data.append( [col.get_text(strip=True) for col in columns])  
        
headers = ["chemicalID", "name", "validity", "date", "chemicalCode"]
#write csv
with open("chemicalVocabulary.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(data)

print("CSV file created successfully.")
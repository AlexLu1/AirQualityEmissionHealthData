# Exploring Greenhouse Gas Emissions by Sector, Air Quality, and Respiratory Health Outcomes in the EU

## Introduction

In this project, we aim to analyze the relationship between greenhouse gas emis-
sions and air quality across EU countries. The primary research goal is whether
high levels of greenhouse gas emissions are an indicator of increased harmful
gas emissions that negatively impact air quality. Additionally, we will examine
respiratory health data to determine whether air quality can serve as a predictor
for these conditions. To achieve this, we will categorize greenhouse emissions by
sector, year and country, investigate harmful gas emissions, and explore lung-
related health conditions to extract meaningful conclusions

## Database Setup
In the main.py file, enter your database connection and select the amount of datasets to be downloaded in the last line
"airQualityDataset.parseAllAirQualityData(True,True,False)".
We recommend setting the first two to true for a download of 4GB and the last one to False, otherwise the size explodes to 800GB. Then just run main.py and the script automatically handles table creation and data parsing. Install dependencies as required.
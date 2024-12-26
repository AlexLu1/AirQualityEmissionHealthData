# Exploring Greenhouse Gas Emissions by Sector, Air Quality, and Respiratory Health Outcomes in the EU

## Introduction

In this project, we aim to analyze the relationship between greenhouse gas emissions and air quality across EU countries.
The primary research goal is whether high levels of greenhouse gas emissions are an indicator of increased harmful
gas emissions that negatively impact air quality. Additionally, we will examine
respiratory health data to determine whether air quality can serve as a predictor
for these conditions. To achieve this, we will categorize greenhouse emissions by
sector, year and country, investigate harmful gas emissions, and explore lung-
related health conditions to extract meaningful conclusions

## Database Setup
Install all dependencies from "requirements.txt" with "pip install -r requirements.txt".
In the main.py file, enter your database connection. Select the amount of datasets to be downloaded in the line "airQualityDataset.parseAllAirQualityData(True,False,False)".
We recommend setting the first two to true for a download of 4GB, the last dataset has a size of 800GB.
Run main.py while the postgresql database is running and the script automatically handles table creation and data parsing.


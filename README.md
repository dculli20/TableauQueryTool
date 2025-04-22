# Tableau Query Tool

## Description:

I built this tool to let Tableau Server/Tableau Online users query from their published data sources. It uses Tableau's Data VizQL Data Service REST API.
This tool allows users to:
1. Select a data source
2. Select dimensions/measures (and aggregations for measures)
3. Add filters
4. See results and download query results to a csv
5. Save queries for re-use
6. Schedule queries to run (the program must be open, so I use Windows Task Scheduler to open it when needed)

## Limitations:

There are a couple of limitations with the API. Some of the following I've run into:
1. Can only process date (not date-time)
2. Names of calculations aren't brought over.
3. Some data sources randomly error out (Still troubleshooting)

## How to make personalize it:

Download the files, and edit the file in the following locations:

### Cluster lines:

Search for {enter_your_cluster}. Should be lines 37, 502, 1107, 1175, 1232, 1281, 1514, 2135, 2436, 2855, 2930

### Token lines:

Search for '{enter_your_token' and replace the token name and secret. Should be lines 1178/1179, 2933/2934

### Site name lines:

Search for 'ContentUrl' and replace {enter_your_site_name} with your site name.

### Splash image (if desired)

Search for 'file path' (line 2969) and replace my file path with yours.

## Get the most out of it:

To take this script and turn it into an application, open cmd and run this (first modify your file path):
pyinstaller --onefile --windowed --name="TableauQueryTool" --add-data "C:/{your_file_path}/TableauQueryMeme.jpg;." --hidden-import=apscheduler.triggers.cron --hidden-import=apscheduler.jobstores.sqlalchemy tableauquerybuilderpublic.py

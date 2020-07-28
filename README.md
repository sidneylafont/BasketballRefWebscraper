# BasketballRefWebscraper

A python script that scrapes all the data (every category for both regular
season and playoff) from https://www.basketball-reference.com/ for all current
NBA players. I provided an example of all the csvs for all the stats (from June
2020) in /webscrapers/csvs-example(6:2020).

I also provided the scripts to load the data into a sqlite database with a
normalized schema in the dataLoading folder.


To get all the updated csvs with up to date stats for all current NBA players:

        $ cd BasketballRefWebscraper
        $ initialize.sh

This will add all the csvs to /webscrapers/csvs

# Packages

- BeautifulSoup
- sqlite3
- urllib
- pandas
- csv
- sys

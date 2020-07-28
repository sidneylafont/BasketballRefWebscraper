import urllib.request as urllib
from bs4 import BeautifulSoup
import string
import csv


def main():
    '''
    scrapes basketball-reference for all the totals stats for active players
    '''
    url = "https://www.basketball-reference.com"
    urlWithPlayer = "https://www.basketball-reference.com/players/"

    letters = list(string.ascii_lowercase)

    players = {}
    index = 0

    for l in letters:
        letterPage = urllib.urlopen(urlWithPlayer + l + "/")
        letterSoup = BeautifulSoup(letterPage.read(), 'html.parser')
        tbodyLetterSoup = letterSoup.find("tbody")
        for p in tbodyLetterSoup.findAll("strong"):
            index = index + 1
            print(index) #about 780 total
            player = {}
            playerPage = urllib.urlopen(url + p.find("a").get("href").strip())
            playerSoup = BeautifulSoup(playerPage.read(), 'html.parser')
            name = playerSoup.find("h1", itemprop="name").text

            playerSoupStr = str(playerSoup)

            startTable = playerSoupStr.find("<tr id=\"playoffs_advanced.")

            playerSoupStr = playerSoupStr[startTable:]

            endtable = playerSoupStr.find("</tbody>")

            playerSoupStr = playerSoupStr[:endtable]

            rows = BeautifulSoup(playerSoupStr, 'html.parser')

            player = []

            for r in rows.findAll("tr"):
                try:
                    yearStats = {}
                    year = r.get("id")[9:]
                    yearStats["year"] = year
                    for cell in r.findAll("td"):
                        yearStats[cell.get("data-stat")] = cell.text
                    player.append(yearStats)
                except:
                    print("Something else went wrong" + str(r))
            players[name] = player

    print(players)

    with open("csvs/playoff_advanced_stats.csv", mode='w') as player_per_game:
        writer = csv.writer(player_per_game)
        for name, years in players.items():
            for year in years:
                writer.writerow([name] + list(year.values()))


main()
import urllib.request as urllib
from bs4 import BeautifulSoup
import string
import csv


def main():
    '''
    scrapes basketball-reference for all the contract information for active players
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

            contactYearsStr = str(playerSoup)

            startYearTable = playerSoupStr.find("<tr class=\"thead\">")

            contactYearsStr = contactYearsStr[startYearTable:]

            endYearTable = playerSoupStr.find("</tr>")

            contactYearsStr = contactYearsStr[:endYearTable]

            yearSoup = BeautifulSoup(contactYearsStr, 'html.parser').findAll("th")

            try:
                year = int(yearSoup[1].text[0:4])

                startTable = playerSoupStr.find("<td><a href=\"/contracts/")

                playerSoupStr = playerSoupStr[startTable:]

                endtable = playerSoupStr.find("</tr>")

                playerSoupStr = playerSoupStr[:endtable]

                rows = BeautifulSoup(playerSoupStr, 'html.parser')

                player = []

                team = rows.find("a").text
                for r in rows.findAll("span"):
                    try:
                        yearStats = {}
                        yearStats["name"] = name
                        yearStats["team"] = team
                        yearStats["year"] = year
                        year = year + 1
                        yearStats["salary"] = r.text
                        player.append(yearStats)
                    except:
                        print("Something else went wrong" + str(r))
                players[name] = player 
            except:
                print(name + ": no salary information")

    print(players)

    with open("csvs/contract.csv", mode='w') as player_per_game:
        writer = csv.writer(player_per_game)
        for name, years in players.items():
            for year in years:
                writer.writerow([name] + list(year.values()))


main()


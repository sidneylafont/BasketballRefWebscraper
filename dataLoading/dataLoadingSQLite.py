import sqlite3

import pandas as pd

import sys


def main():
    conn = create_connection('BBSQLitedb.db')
    cur = conn.cursor()

    perGame = pd.read_csv("../webscrapers/csvs/player_per_game.csv",
                          names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                 '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    perGame = perGame.drop(columns='league')

    playerIDDict = {}

    id = 0
    for n in perGame['name']:
        if not (n in playerIDDict.keys()):
            playerIDDict[n] = id
            id += 1

    insertPlayersStr = "INSERT INTO `Players` (\'pid\', \'name\') VALUES\n"

    for key, value in playerIDDict.items():
        insertPlayersStr = insertPlayersStr + "(" + str(value) + ", \"" + key + "\"),\n"

    insertPlayersStr = insertPlayersStr[:-2] + ";"
    cur.execute(insertPlayersStr)
    conn.commit()

    insertPerGameStr = "INSERT INTO `PerGame` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                       "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                       "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                       "\'PF\', \'PTS\') VALUES\n"

    for index, row in perGame.iterrows():
        insertPerGameStr = insertPerGameStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertPerGameStr = insertPerGameStr + "\'" + str(values[i]) + "\', "
            else:
                insertPerGameStr = insertPerGameStr + str(values[i]) + ", "
        insertPerGameStr = insertPerGameStr[:-2] + "),\n"

    insertPerGameStr = insertPerGameStr[:-2] + ";"
    insertPerGameStr = insertPerGameStr.replace("nan", "NULL")
    cur.execute(insertPerGameStr)
    conn.commit()

    Totals = pd.read_csv("../webscrapers/csvs/player_total.csv",
                          names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                 '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    Totals = Totals.drop(columns='league')

    insertTotalsStr = "INSERT INTO `Totals` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                       "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                       "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                       "\'PF\', \'PTS\') VALUES\n"

    for index, row in Totals.iterrows():
        insertTotalsStr = insertTotalsStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertTotalsStr = insertTotalsStr + "\'" + str(values[i]) + "\', "
            else:
                insertTotalsStr = insertTotalsStr + str(values[i]) + ", "
        insertTotalsStr = insertTotalsStr[:-2] + "),\n"

    insertTotalsStr = insertTotalsStr[:-2] + ";"
    insertTotalsStr = insertTotalsStr.replace("nan", "NULL")
    cur.execute(insertTotalsStr)
    conn.commit()

    per36 = pd.read_csv("../webscrapers/csvs/per_36.csv",
                        names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                               'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                               '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                               'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    per36 = per36.drop(columns='league')

    insertPer36Str = "INSERT INTO `Per36` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                     "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                     "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                     "\'PF\', \'PTS\') VALUES\n"

    for index, row in per36.iterrows():
        insertPer36Str = insertPer36Str + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 29):
            if i == 1:
                insertPer36Str = insertPer36Str + str(values[i][-2:]) + ", "
            elif i == 3 or i == 4:
                insertPer36Str = insertPer36Str + "\'" + str(values[i]) + "\', "
            else:
                insertPer36Str = insertPer36Str + str(values[i]) + ", "
        insertPer36Str = insertPer36Str[:-2] + "),\n"

    insertPer36Str = insertPer36Str[:-2] + ";"
    insertPer36Str = insertPer36Str.replace("nan", "NULL")
    cur.execute(insertPer36Str)
    conn.commit()

    per100Poss = pd.read_csv("../webscrapers/csvs/per_100_poss.csv",
                             names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                    'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                    '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                                    'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'B', 'ORTG', 'DRTG'],
                             index_col=False, header=None)
    per100Poss = per100Poss.drop(columns='league').drop(columns='B')

    insertPer100PossStr = "INSERT INTO `Per100Poss` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                          "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                          "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                          "\'PF\', \'PTS\', \'ORTG\', \'DRTG\') VALUES\n"

    for index, row in per100Poss.iterrows():
        insertPer100PossStr = insertPer100PossStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 31):
            if i == 3 or i == 4:
                insertPer100PossStr = insertPer100PossStr + "\'" + str(values[i]) + "\', "
            else:
                insertPer100PossStr = insertPer100PossStr + str(values[i]) + ", "
        insertPer100PossStr = insertPer100PossStr[:-2] + "),\n"

    insertPer100PossStr = insertPer100PossStr[:-2] + ";"
    insertPer100PossStr = insertPer100PossStr.replace("nan", "NULL")
    cur.execute(insertPer100PossStr)
    conn.commit()

    Advanced = pd.read_csv("../webscrapers/csvs/advanced_stats.csv",
                           names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                  'MP', 'PER', 'TS%', '3PAR', 'FTAR', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%',
                                  'BLK%', 'TOV%', 'USG%', 'B1', 'OWS', 'DWS', 'WS', 'WS/48', 'B2', 'OBPM', 'DBPM',
                                  'BPM',
                                  'VORP'],
                           index_col=False, header=None)
    Advanced = Advanced.drop(columns='league').drop(columns='B1').drop(columns='B2')

    insertAdvancedStr = "INSERT INTO `Advanced` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                        "\'PER\', \'TS%\', \'3PAR\', \'FTR\', \'ORB%\', \'DRB%\', \'TRB%\', \'AST%\', \'STL%\', " \
                        "\'BLK%\', \'TOV%\', \'USG%\', \'OWS\', \'DWS\', \'WS\', \'WS/48\', \'OBPM\', \'DBPM\', \'BPM\', " \
                        "\'VORP\') VALUES\n"

    for index, row in Advanced.iterrows():
        insertAdvancedStr = insertAdvancedStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 27):
            if i == 3 or i == 4:
                insertAdvancedStr = insertAdvancedStr + "\'" + str(values[i]) + "\', "
            else:
                insertAdvancedStr = insertAdvancedStr + str(values[i]) + ", "
        insertAdvancedStr = insertAdvancedStr[:-2] + "),\n"

    insertAdvancedStr = insertAdvancedStr[:-2] + ";"
    insertAdvancedStr = insertAdvancedStr.replace("nan", "NULL")
    cur.execute(insertAdvancedStr)
    conn.commit()

    Shooting = pd.read_csv("../webscrapers/csvs/shooting.csv",
                           names=['name', 'year', 'age', 'team', 'league', 'position', 'GP', 'MP', 'FG%', 'DIST',
                                  '2P%ofFGBD', '0-3%ofFGBD', '3-10%ofFGBD', '10-16%ofFGBD', '16-3PT%ofFGBD',
                                  '3P%ofFGBD', '2PFG%BD', '0-3FG%BD', '3-10FG%BD', '10-16FG%BD', '16-3PTFG%BD',
                                  '3PFG%BD', 'D%ASTD', 'D%FGA', 'DMD', '3%ASTD', '3%3PA', '3P%', '3ATT', '3MD'],
                           index_col=False, header=None)
    Shooting = Shooting.drop(columns='league')

    insertShootingStr = "INSERT INTO `Shooting` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                        "\'FG%\', \'DIST\', \'2P%ofFGBD\', \'0-3%ofFGBD\', \'3-10%ofFGBD\', \'10-16%ofFGBD\', " \
                        "\'16-3PT%ofFGBD\', \'3P%ofFGBD\', \'2PFG%BD\', \'0-3FG%BD\', \'3-10FG%BD\', \'10-16FG%BD\'," \
                        " \'16-3PTFG%BD\', \'3PFG%BD\', \'D%ASTD\', \'D%FGA\', \'DMD\', \'3%ASTD\', \'3%3PA\', " \
                        "\'3P%\', \'3ATT\', \'3MD\') VALUES\n"

    for index, row in Shooting.iterrows():
        insertShootingStr = insertShootingStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 29):
            if i == 3 or i == 4:
                insertShootingStr = insertShootingStr + "\'" + str(values[i]) + "\', "
            else:
                insertShootingStr = insertShootingStr + str(values[i]) + ", "
        insertShootingStr = insertShootingStr[:-2] + "),\n"

    insertShootingStr = insertShootingStr[:-2] + ";"
    insertShootingStr = insertShootingStr.replace("nan", "NULL")
    cur.execute(insertShootingStr)
    conn.commit()

    PBP = pd.read_csv("../webscrapers/csvs/play_by_play.csv",
                      names=['name', 'year', 'age', 'team', 'league', 'position', 'GP', 'MP', 'PG%', 'SG%',
                             'SF%', 'PF%', 'C%', 'OnCourt', 'OnOff', 'BadPass', 'LostBall', 'FCShoot',
                             'FCOff', 'FDShoot', 'FDOff', 'PGA', 'And1', 'Blkd'],
                      index_col=False, header=None)
    PBP = PBP.drop(columns='league')

    insertPBPStr = "INSERT INTO `PlayByPlay` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                   "\'PG%\', \'SG%\', \'SF%\', \'PF%\', \'C%\', \'OnCourt\', " \
                   "\'OnOff\', \'BadPass\', \'LostPass\', \'FCShoot\', \'FCOff\', \'FDShoot\'," \
                   " \'FDOff\', \'PGA\', \'And1\', \'Blkd\') VALUES\n"

    for index, row in PBP.iterrows():
        insertPBPStr = insertPBPStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 23):
            if i == 3 or i == 4:
                insertPBPStr = insertPBPStr + "\'" + str(values[i]) + "\', "
            else:
                insertPBPStr = insertPBPStr + str(values[i]).replace('%', '') + ", "
        insertPBPStr = insertPBPStr[:-2] + "),\n"

    insertPBPStr = insertPBPStr[:-2] + ";"
    insertPBPStr = insertPBPStr.replace("nan", "NULL")
    cur.execute(insertPBPStr)
    conn.commit()

    Contracts = pd.read_csv("../webscrapers/csvs/contract.csv",
                            names=['name', 'name2', 'team', 'year', 'salary'],
                            index_col=False, header=None)
    Contracts = Contracts.drop(columns='name2')

    insertContractsStr = "INSERT INTO `Contract` (\'pid\', \'team\', \'year\', \'salary\') Values\n"

    teamToAbv = {"Atlanta Hawks": "ATL", "Brooklyn Nets": "BKN", "Boston Celtics": "BOS", "Charlotte Hornets": "CHA",
                 "Chicago Bulls": "CHA", "Cleveland Cavaliers": "CLE", "Dallas Mavericks": "DAL",
                 "Denver Nuggets": "DEN",
                 "Detroit Pistons": "DET", "Golden State Warriors": "GSW", "Houston Rockets": "HOU",
                 "Indiana Pacers": "IND",
                 "Los Angeles Clippers": "LAC", "Los Angeles Lakers": "LAL", "Memphis Grizzlies": "MEM",
                 "Miami Heat": "MIA",
                 "Milwaukee Bucks": "MIL", "Minnesota Timberwolves": "MIN", "New Orleans Pelicans": "NOP",
                 "New York Knicks": "NYK", "Oklahoma City Thunder": "OKC", "Orlando Magic": "ORL",
                 "Philadelphia 76ers": "PHI",
                 "Phoenix Suns": "PHX", "Portland Trail Blazers": "POR", "Sacramento Kings": "SAC",
                 "San Antonio Spurs": "SAS",
                 "Toronto Raptors": "TOR", "Utah Jazz": "UTA", "Washington Wizards": "WAS"}

    for index, row in Contracts.iterrows():
        insertContractsStr = insertContractsStr + "(" + str(playerIDDict[row["name"]]) + ", \'" + teamToAbv[
            row["team"]] + \
                             "\', " + str(row["year"]) + ", " + str(row["salary"]).replace(',', '').replace('$',
                                                                                                            '') + "),\n"

    insertContractsStr = insertContractsStr[:-2] + ";"
    insertContractsStr = insertContractsStr.replace("nan", "NULL")
    cur.execute(insertContractsStr)
    conn.commit()

    PlayoffPerGame = pd.read_csv("../webscrapers/csvs/playoff_per_game.csv",
                                 names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                        'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                        '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                        'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    PlayoffPerGame = PlayoffPerGame.drop(columns='league')

    insertPlayoffPerGameStr = "INSERT INTO `PlayoffPerGame` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                              "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                              "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                              "\'PF\', \'PTS\') VALUES\n"

    for index, row in PlayoffPerGame.iterrows():
        insertPlayoffPerGameStr = insertPlayoffPerGameStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertPlayoffPerGameStr = insertPlayoffPerGameStr + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffPerGameStr = insertPlayoffPerGameStr + str(values[i]) + ", "
        insertPlayoffPerGameStr = insertPlayoffPerGameStr[:-2] + "),\n"

    insertPlayoffPerGameStr = insertPlayoffPerGameStr[:-2] + ";"
    insertPlayoffPerGameStr = insertPlayoffPerGameStr.replace("nan", "NULL").replace("per_game.", "")
    cur.execute(insertPlayoffPerGameStr)
    conn.commit()

    PlayoffTotals = pd.read_csv("../webscrapers/csvs/playoff_total.csv",
                                names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                       'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                       '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                       'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    PlayoffTotals = PlayoffTotals.drop(columns='league')

    insertPlayoffTotalsStr = "INSERT INTO `PlayoffTotals` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                             "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                             "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                             "\'PF\', \'PTS\') VALUES\n"

    for index, row in PlayoffTotals.iterrows():
        insertPlayoffTotalsStr = insertPlayoffTotalsStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertPlayoffTotalsStr = insertPlayoffTotalsStr + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffTotalsStr = insertPlayoffTotalsStr + str(values[i]) + ", "
        insertPlayoffTotalsStr = insertPlayoffTotalsStr[:-2] + "),\n"

    insertPlayoffTotalsStr = insertPlayoffTotalsStr[:-2] + ";"
    insertPlayoffTotalsStr = insertPlayoffTotalsStr.replace("nan", "NULL").replace("totals.", "")
    cur.execute(insertPlayoffTotalsStr)
    conn.commit()

    PlayoffPer36 = pd.read_csv("../webscrapers/csvs/playoff_per_36.csv",
                               names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                      'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                      '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                                      'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    PlayoffPer36 = PlayoffPer36.drop(columns='league')

    insertPlayoffPer36Str = "INSERT INTO `PlayoffPer36` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                            "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                            "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                            "\'PF\', \'PTS\') VALUES\n"

    for index, row in PlayoffPer36.iterrows():
        insertPlayoffPer36Str = insertPlayoffPer36Str + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 29):
            if i == 1:
                insertPlayoffPer36Str = insertPlayoffPer36Str + str(values[i][-2:]) + ", "
            elif i == 3 or i == 4:
                insertPlayoffPer36Str = insertPlayoffPer36Str + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffPer36Str = insertPlayoffPer36Str + str(values[i]) + ", "
        insertPlayoffPer36Str = insertPlayoffPer36Str[:-2] + "),\n"

    insertPlayoffPer36Str = insertPlayoffPer36Str[:-2] + ";"
    insertPlayoffPer36Str = insertPlayoffPer36Str.replace("nan", "NULL")
    cur.execute(insertPlayoffPer36Str)
    conn.commit()

    PlayoffPer100Poss = pd.read_csv("../webscrapers/csvs/playoff_per_100_poss.csv",
                                    names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                           'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                           '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                                           'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'B', 'ORTG',
                                           'DRTG'], index_col=False, header=None)
    PlayoffPer100Poss = PlayoffPer100Poss.drop(columns='league').drop(columns='B')

    insertPlayoffPer100PossStr = "INSERT INTO `PlayoffPer100Poss` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                                 "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                                 "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                                 "\'PF\', \'PTS\', \'ORTG\', \'DRTG\') VALUES\n"

    for index, row in PlayoffPer100Poss.iterrows():
        insertPlayoffPer100PossStr = insertPlayoffPer100PossStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 31):
            if i == 3 or i == 4:
                insertPlayoffPer100PossStr = insertPlayoffPer100PossStr + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffPer100PossStr = insertPlayoffPer100PossStr + str(values[i]) + ", "
        insertPlayoffPer100PossStr = insertPlayoffPer100PossStr[:-2] + "),\n"

    insertPlayoffPer100PossStr = insertPlayoffPer100PossStr[:-2] + ";"
    insertPlayoffPer100PossStr = insertPlayoffPer100PossStr.replace("nan", "NULL").replace("per_poss.", "")
    cur.execute(insertPlayoffPer100PossStr)
    conn.commit()

    PlayoffAdvanced = pd.read_csv("../webscrapers/csvs/playoff_advanced_stats.csv",
                                  names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                         'MP', 'PER', 'TS%', '3PAR', 'FTAR', 'ORB%', 'DRB%', 'TRB%', 'AST%', 'STL%',
                                         'BLK%', 'TOV%', 'USG%', 'B1', 'OWS', 'DWS', 'WS', 'WS/48', 'B2', 'OBPM',
                                         'DBPM', 'BPM',
                                         'VORP'],
                                  index_col=False, header=None)
    PlayoffAdvanced = PlayoffAdvanced.drop(columns='league').drop(columns='B1').drop(columns='B2')

    insertPlayoffAdvancedStr = "INSERT INTO `PlayoffAdvanced` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                               "\'PER\', \'TS%\', \'3PAR\', \'FTR\', \'ORB%\', \'DRB%\', \'TRB%\', \'AST%\', \'STL%\', " \
                               "\'BLK%\', \'TOV%\', \'USG%\', \'OWS\', \'DWS\', \'WS\', \'WS/48\', \'OBPM\', \'DBPM\', \'BPM\', " \
                               "\'VORP\') VALUES\n"

    for index, row in PlayoffAdvanced.iterrows():
        insertPlayoffAdvancedStr = insertPlayoffAdvancedStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 27):
            if i == 3 or i == 4:
                insertPlayoffAdvancedStr = insertPlayoffAdvancedStr + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffAdvancedStr = insertPlayoffAdvancedStr + str(values[i]) + ", "
        insertPlayoffAdvancedStr = insertPlayoffAdvancedStr[:-2] + "),\n"

    insertPlayoffAdvancedStr = insertPlayoffAdvancedStr[:-2] + ";"
    insertPlayoffAdvancedStr = insertPlayoffAdvancedStr.replace("nan", "NULL").replace("advanced.", "")
    cur.execute(insertPlayoffAdvancedStr)
    conn.commit()

    PlayoffShooting = pd.read_csv("../webscrapers/csvs/playoff_shooting.csv",
                                  names=['name', 'year', 'age', 'team', 'league', 'position', 'GP', 'MP', 'FG%',
                                         'DIST',
                                         '2P%ofFGBD', '0-3%ofFGBD', '3-10%ofFGBD', '10-16%ofFGBD', '16-3PT%ofFGBD',
                                         '3P%ofFGBD', '2PFG%BD', '0-3FG%BD', '3-10FG%BD', '10-16FG%BD',
                                         '16-3PTFG%BD',
                                         '3PFG%BD', 'D%ASTD', 'D%FGA', 'DMD', '3%ASTD', '3%3PA', '3P%', '3ATT',
                                         '3MD'],
                                  index_col=False, header=None)
    PlayoffShooting = PlayoffShooting.drop(columns='league')

    insertPlayoffShootingStr = "INSERT INTO `PlayoffShooting` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                               "\'FG%\', \'DIST\', \'2P%ofFGBD\', \'0-3%ofFGBD\', \'3-10%ofFGBD\', \'10-16%ofFGBD\', " \
                               "\'16-3PT%ofFGBD\', \'3P%ofFGBD\', \'2PFG%BD\', \'0-3FG%BD\', \'3-10FG%BD\', \'10-16FG%BD\'," \
                               " \'16-3PTFG%BD\', \'3PFG%BD\', \'D%ASTD\', \'D%FGA\', \'DMD\', \'3%ASTD\', \'3%3PA\', " \
                               "\'3P%\', \'3ATT\', \'3MD\') VALUES\n"

    for index, row in PlayoffShooting.iterrows():
        insertPlayoffShootingStr = insertPlayoffShootingStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 29):
            if i == 3 or i == 4:
                insertPlayoffShootingStr = insertPlayoffShootingStr + "\'" + str(values[i]) + "\', "
            else:
                insertPlayoffShootingStr = insertPlayoffShootingStr + str(values[i]) + ", "
        insertPlayoffShootingStr = insertPlayoffShootingStr[:-2] + "),\n"

    insertPlayoffShootingStr = insertPlayoffShootingStr[:-2] + ";"
    insertPlayoffShootingStr = insertPlayoffShootingStr.replace("nan", "NULL").replace("shooting.", "")
    cur.execute(insertPlayoffShootingStr)
    conn.commit()

    PPBP = pd.read_csv("../webscrapers/csvs/playoff_play_by_play.csv",
                       names=['name', 'year', 'age', 'team', 'league', 'position', 'GP', 'MP', 'PG%', 'SG%',
                              'SF%', 'PF%', 'C%', 'OnCourt', 'OnOff', 'BadPass', 'LostBall', 'FCShoot',
                              'FCOff', 'FDShoot', 'FDOff', 'PGA', 'And1', 'Blkd'],
                       index_col=False, header=None)
    PPBP = PPBP.drop(columns='league')

    insertPPBPStr = "INSERT INTO `PlayoffPlayByPlay` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'MP\', " \
                    "\'PG%\', \'SG%\', \'SF%\', \'PF%\', \'C%\', \'OnCourt\', " \
                    "\'OnOff\', \'BadPass\', \'LostPass\', \'FCShoot\', \'FCOff\', \'FDShoot\'," \
                    " \'FDOff\', \'PGA\', \'And1\', \'Blkd\') VALUES\n"

    for index, row in PPBP.iterrows():
        insertPPBPStr = insertPPBPStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 23):
            if i == 3 or i == 4:
                insertPPBPStr = insertPPBPStr + "\'" + str(values[i]) + "\', "
            else:
                insertPPBPStr = insertPPBPStr + str(values[i]).replace('%', '') + ", "
        insertPPBPStr = insertPPBPStr[:-2] + "),\n"

    insertPPBPStr = insertPPBPStr[:-2] + ";"
    insertPPBPStr = insertPPBPStr.replace("nan", "NULL").replace("pbp.", "")
    cur.execute(insertPPBPStr)
    conn.commit()

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except:
        print(sys.exc_info()[0])

    return conn


main()

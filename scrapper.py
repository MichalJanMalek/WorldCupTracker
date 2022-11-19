import requests
from bs4 import BeautifulSoup
from requests_html import HTMLSession
import pandas as pd
import mariadb 

# setting up URL to scrape
URL = "https://www.fifa.com/fifaplus/en/tournaments/mens/worldcup/qatar2022/scores-fixtures"
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.35', "Referer": "https://www.fifa.com"}

# code that actually scrapes the website
'''
# set up session and read page
s = HTMLSession()
page = s.get(URL, headers=headers)
print("Page status: ", page)

# attempt to read html from page after 5 seconds of waiting for js to load data
page.html.render(retries=1, wait=5, sleep=5)   
soup = BeautifulSoup(page.html.raw_html, "html.parser")

# save html to file to use later
with open('readme.html', 'w', encoding='utf-8') as f:
    f.write(str(soup.prettify()))
'''

# use preexisting html file to not have to constantly scrape page while testing
contents = None
with open('readme.html', 'r') as f:
    contents = f.read()

soup = BeautifulSoup(contents, "html.parser")

# set up dataframe for storing info about matches
matches = pd.DataFrame(columns=['Group','TeamA','TeamB','TeamA_Score','TeamB_Score','Date','Match_Time'])

# the div that holds info about each game in seperate day divs
days = soup.find_all("div", class_="col-xl-12 col-lg-12 ff-pb-24 ff-text-blue-dark col-md-12 col-sm-12")

# loop through all "days" divs, which seperates out whichday each game is on
for day in days:
    # div with the date of match in it, seperate out month day and then create a date attribute for in mysql format (YYYY-MM-DD)
    date = day.find("div", class_="matches-container_title__1uTPf").text.strip()[:-5]
    date_month = date[-3:]
    date_day   = date[:-3]
    date_fin = "2022-"
    if date_month == "Nov":
        date_fin += "11-"
    else:
        date_fin += "12-"
    date_fin += date_day

    # loop through all games that are found in the day div, with this specific div seperating out all matches on that day
    for match in day.find_all("div", class_="match-block_MatchBlock__2fDak match-block_wtwMatchBlock__3rTRv match-block_borderless__2lXuY"):
        group = match.find("div", class_="match-block_wtwStadiumName__2EACw ff-mb-0").text.strip()
        teamA = match.find("div", class_="wtw-teams-horizontally-component_team2__-ZMT3").text.strip()
        teamB = match.find("div", class_="wtw-teams-horizontally-component_team1__3bRzY").text.strip()
        match_time = match.find("div", class_="wtw-teams-horizontally-component_status__ZK_Cl").text.strip()

        # for the score, if there is no number, match hasn't started yet, set to null
        teamA_score = match.find("div", class_="wtw-teams-horizontally-component_score1__3HTmk").text.strip()
        if teamA_score == "":
            teamA_score = None
        teamB_score = match.find("div", class_="wtw-teams-horizontally-component_score2__20sPm").text.strip()
        if teamB_score == "":
            teamB_score = None

        # add the match to the dataframe
        matches.loc[len(matches.index)] = [group,teamA,teamB,teamA_score,teamB_score,date_fin,match_time]

# find all non-playoff matches (gets all group stage matches)
non_playoff_matches = matches.where(matches['Group'].str.contains(pat="Group")).dropna(how="all")

# finds a list of all teams participating in the cup
teams = (pd.concat([non_playoff_matches['TeamA'],non_playoff_matches['TeamB']])).dropna().unique()
teams.sort()

# set up a dataframe for individual team info
teams_pd = pd.DataFrame(columns=['Name','Group','Games_Played','Wins','Ties','Losses','Points'])

# add all teams to the teams dataframe
for team in teams:
    teams_pd.loc[len(teams_pd.index)] = [team, None, 0, 0, 0, 0, 0]

# add their group letter
for index, match in pd.DataFrame(non_playoff_matches).iterrows():
    teamA = teams_pd.loc[teams_pd['Name'] == match['TeamA']]
    teamB = teams_pd.loc[teams_pd['Name'] == match['TeamB']]

    teams_pd.loc[teams_pd['Name'] == match['TeamA'],['Group']] = match['Group'][-1]

# create a connection to mariadb

with open('config.txt', 'r') as f:
    contents = f.readlines()

info = {}
for line in contents:
    split = line.split('=')
    info[split[0]] = split[1].strip()

try:
    conn = mariadb.connect(
        user=info['user'],
        password=info['pass'],
        host=info['host'],
        database=info['db'])
except Exception as e:
    print(e)
    exit()

cur = conn.cursor()

# setup a list of all teams into a list of tuples, then execute as an insert
teams_tup = []
for idx, i in teams_pd.iterrows():
    teams_tup.append(tuple(i))

try: 
    cur.executemany("REPLACE INTO teams (name, group_letter, games_played, wins, ties, losses, points) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                teams_tup) 
except mariadb.Error as e: 
    print(f"Error: {e}")

conn.commit() 

# setup a list of all matches into a list of lists, then execute as an insert
match_tup = []
for idx, i in matches.iterrows():
    match_tup.append(list(i)[1:])

try: 
    cur.executemany("REPLACE INTO matches (TeamA,TeamB,TeamA_Score,TeamB_Score,date,time) VALUES (?, ?, ?, ?, ?, ?)",
                                match_tup) 
except mariadb.Error as e: 
    print(f"Error: {e}")

conn.commit() 

# done scraping
print("Updated Data")
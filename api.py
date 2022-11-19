import pandas as pd
import mariadb 

def get_db_cur():
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

    return conn.cursor()

def get_team_info(team_name):
    cur = get_db_cur()
    cur.execute("SELECT * FROM teams WHERE name=?", (team_name,)) 
    return cur

def get_team_info(team_name, info):
    cur = get_db_cur()
    cur.execute("SELECT " + info + " FROM teams WHERE name=?", [team_name]) 
    return list(cur.fetchone())

def get_group_table(group):
    cur = get_db_cur()
    cur.execute("SELECT * FROM teams WHERE group_letter=?", [group]) 
    return pd.DataFrame(cur.fetchall())
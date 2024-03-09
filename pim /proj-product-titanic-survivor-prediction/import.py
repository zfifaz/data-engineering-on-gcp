import configparser

import pandas as pd


CONFIG_FILE = "db.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

database = parser.get("mysql_config", "database")
user = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
host = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"

df = pd.read_csv("titanic-original.csv")
df.to_sql("titanic", con=uri, if_exists="replace", index=False)
print(f"Imported titanic data successfully")
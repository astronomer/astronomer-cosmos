import sqlite3

import pandas as pd

df = pd.read_csv("imdb.csv")
conn = sqlite3.connect("imdb.db")
df.to_sql("movies_ratings", conn, if_exists="replace", index=False)

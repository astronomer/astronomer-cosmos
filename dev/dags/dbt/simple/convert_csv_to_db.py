import pandas as pd
import sqlite3

df = pd.read_csv("imdb.csv")
conn = sqlite3.connect("imdb.db")
df.to_sql("movies_ratings", conn, if_exists="replace", index=False)

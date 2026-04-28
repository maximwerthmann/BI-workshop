##################
# Initialization #
##################

from supabase import create_client
from datetime import datetime
import requests
import pandas as pd
from bs4 import BeautifulSoup
import os

print("Starting Maxim's web scraping tool...")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

url = "https://news.ycombinator.com/"

response = requests.get(url, timeout=20)
response.encoding = "utf-8"
soup = BeautifulSoup(response.text, "html.parser")

rows = []


def scrape_data():
    for item in soup.select(".athing"):
      subtext = item.find_next_sibling()


      # Extract the fields relevant to your business question
      rank_el = item.select_one(".rank")

      if rank_el:
        raw_text = rank_el.get_text(strip=True)
        rank = int(raw_text.split(".")[0])

      title_el = item.select_one(".titleline")

      score_el = subtext.select_one(".score")
      if score_el:
        raw_text = score_el.get_text(strip=True)
        score = int(raw_text.split()[0])
      else:
        score = None


      age_el = subtext.select_one(".age")

      if age_el:
        date_el = age_el.get("title")
      else:
        date_el = None

      comments_el = subtext.select_one(".subline > a:last-of-type")

      if comments_el:
        raw_text = comments_el.get_text(strip=True)

        # Check if it actually has comments, since 0 comments usually says "discuss"
        if "comment" in raw_text:
          # Splits "70\xa0comments" into ["70", "comments"] and grabs the "70"
          number_string = raw_text.split("\xa0")[0]
          comments = int(number_string) # Convert it to an actual math number!
        else:
          comments = 0
      else:
        comments = 0

      author_el = subtext.select_one(".hnuser")


      title = title_el.get_text(strip=True) if title_el else None

      date = date_el if date_el else None # Removed .get_text(strip=True)

      author = author_el.get_text(strip=True) if author_el else None


      rows.append({
         "rank": rank,
         "title": title,
         "score": score,
         "date": date,
         "comments": comments,
         "author": author
      })

scrape_data()



df = pd.DataFrame(rows)


############
# Supabase #
############


supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
scrape_time = datetime.now().isoformat()


rows_to_append = []

for _, row in df.iterrows():

  rows_to_append.append({
      "rank":        int(row.get("rank", 0)),
      "title":       str(row.get("title", "")),
      "score":       int(row.get("score", 0)),
      "date":        str(row.get("date", "")),
      "comments":    int(row.get("comments", 0)),
      "author":      str(row.get("author", "")),
      "scraped_at":  scrape_time
  })

result = supabase.table("posts").insert(rows_to_append).execute()

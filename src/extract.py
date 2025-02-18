# Connect to a rapidAPI movie database and extract data
# Return everything as a json object
# Return  every movie info untouched

import requests
import json
import os
from dotenv import load_dotenv


def extract_data():
   load_dotenv()
   url = os.getenv('URL')
   headers = {
      'x-rapidapi-key': os.getenv('API_KEY'),
      'x-rapidapi-host': os.getenv('API_HOST')
   }
   response = requests.request("GET", url, headers=headers)
   return json.loads(response.text)

# Return a list of movies as a list of dictionaries
def fetch_movies(n: int) -> list:
   url = "https://movie-database-alternative.p.rapidapi.com/"
   
   headers = {
      "X-RapidAPI-Key": "56bb1753f3msh8751505a34df417p10122fjsn0811d04462a4",
      "X-RapidAPI-Host": "movie-database-alternative.p.rapidapi.com"
   }

   # Load the last imdb id fetched
   last_id = 1
   with open("info.json", "r") as f:
      last_id = json.load(f)["last_imdb_id"]

   not_found = 0
   movies = []
   for i in range(int(last_id) + 1, int(last_id) + n + 1):
      params = {"i": f"tt{i:07d}", "r": "json"}  # tt4154796 = Avengers: Endgame
      print(params)
      response = requests.get(url, headers=headers, params=params)

      if response.status_code == 200:
         data = response.json()
         movies.append(data)
         print(data)  # Exibe a resposta formatada
      else:
         not_found += 1
         print(f"Erro na requisição: {response.status_code}")


   with open("info.json", "w") as f:
      json.dump({"last_imdb_id": i}, f)

   print(f"Não encontrados: {not_found}")

   return movies


if __name__ == "__main__":
   movies = fetch_movies(5)
   print(movies)
   #print(extract_data())
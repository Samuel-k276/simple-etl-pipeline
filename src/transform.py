from extract import fetch_movies

def trim_data(movies: list) -> list:
   trimmed_movies = []

   for movie in movies:
      minutes = movie['Runtime'].split()[0]
      if minutes == 'N/A': minutes = 0
      rating = next((rate['Value'] for rate in movie['Ratings'] if rate['Source'] in ['Internet Movie Database', 'IMDb']), 0)
      if rating != 0: rating = rating.split('/')[0]
      trimmed_movies.append({
         'title': movie['Title'],
         'year': movie['Year'],
         'genre': movie['Genre'],
         'director': movie['Director'],
         'minutes': int(minutes),
         'rating': float(rating)
      })
      
   return trimmed_movies

def main():
   movies = fetch_movies(5)
   trimmed = trim_data(movies)
   print(trimmed)


if __name__ == "__main__":
   main()
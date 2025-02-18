# Load the transformed data into the database
# Currently the database is a local PostgreSQL database

import psycopg2
import os
from dotenv import load_dotenv
from transform import trim_data
from extract import fetch_movies

def connection_params():
   load_dotenv()
   return {
      'dbname': os.getenv('DBNAME'),    
      'user': os.getenv('USERNAME'),
      'password': os.getenv('PASSWORD'),  
      'host': os.getenv('HOST'),          
      'port': os.getenv('PORT')         
   }

def connect(conn_params) -> psycopg2.extensions.connection:
   try:
      conn = psycopg2.connect(**conn_params)
      print("Conexão bem-sucedida!")
      return conn
   except Exception as e:
      print(f"Erro ao conectar ao banco de dados: {e}")
      return None

def get_cursor(conn) -> psycopg2.extensions.cursor:
   try:
      cur = conn.cursor()
      return cur
   except Exception as e:
      print(f"Erro ao obter cursor: {e}")
      return None
   
def insert_movie(movie_data, cursor) -> bool:
   try:
      cursor.execute(
         "INSERT INTO movies_table (name, year, genre, duration_minutes, director, imdb_rating) VALUES (%s, %s, %s, %s, %s, %s);", 
         (movie_data['title'], movie_data['year'], movie_data['genre'], movie_data['minutes'], movie_data['director'], float(movie_data['rating']))
      )
      cursor.connection.commit()

      return True
   except Exception as e:
      print(f"Erro ao inserir filme: {e}")
      return False

def insert_movies(movies_data, cursor) -> int:
   inserts_failed = 0
   for movie in movies_data:
      if not insert_movie(movie, cursor):
         inserts_failed += 1
      else:
         print(f"Filme {movie['title']} inserido com sucesso!")
   
   return inserts_failed


def main():
   # Parâmetros de conexão (variavel de ambiente)
   conn_params = connection_params()

   # Conectar ao banco de dados
   conn = connect(conn_params)
   if conn is None:
      exit()
   
   # Obter um cursor
   cur = get_cursor(conn)
   if cur is None:
      exit()

   # Obter os dados transformados
   movies = trim_data(fetch_movies(50))

   # Inserir os dados no banco de dados
   failed = insert_movies(movies, cur)
   print(f"Operação finalizada com {failed} falhas.")

if __name__ == "__main__":
   main()
   # see all in the table

   # Executar uma consulta
   conn = connect(connection_params())
   if conn is None:
      exit()
   
   # Obter um cursor
   cur = get_cursor(conn)
   if cur is None:
      exit()
   
   cur.execute("SELECT * FROM movies_table")
   rows = cur.fetchall()
   for row in rows:
      print(row)

   
   

import psycopg2
from load import connection_params, connect, get_cursor, insert_movies
from extract import fetch_movies
from transform import trim_data

def main():
   # Connection parameters (environment variable)
   conn_params = connection_params()

   # Connect to the database
   conn = connect(conn_params)
   if conn is None:
      exit()
   
   # Get a cursor
   cur = get_cursor(conn)
   if cur is None:
      exit()

   # Get the transformed data
   movies = trim_data(fetch_movies(50))

   # Insert the data into the database
   failed = insert_movies(movies, cur)
   print(f"Operation completed with {failed} failures.")

   # Close the cursor and the connection
   cur.close()
   conn.close()

if __name__ == "__main__":
   main()
import numpy as np
import pickle

import psycopg2 as pg
import pandas.io.sql as psql
import pandas as pd

from typing import Union, List, Tuple

connection = pg.connect(host='pgsql-196447.vipserv.org', port=5432, dbname='wbauer_adb', user='wbauer_adb', password='adb2020');

def film_in_category(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str, dokładnie taki jak podana wartość
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) == str:
        result = pd.read_sql(f"SELECT film.title, language.name as languge, category.name as category\
                          FROM film \
                          INNER JOIN film_category \
                          ON film.film_id = film_category.film_id \
                          INNER JOIN category \
                          ON film_category.category_id = category.category_id \
                          INNER JOIN language \
                          ON film.language_id = language.language_id \
                          WHERE category.name IN ('{category}')\
                          ORDER BY film.title, language.name", con=connection)
    else:
        if type(category) == int:
            result = pd.read_sql(f"SELECT film.title, language.name as languge, category.name as category\
                          FROM film \
                          INNER JOIN film_category \
                          ON film.film_id = film_category.film_id \
                          INNER JOIN category \
                          ON film_category.category_id = category.category_id \
                          INNER JOIN language \
                          ON film.language_id = language.language_id \
                          WHERE category.category_id = {category}\
                          ORDER BY film.title, language.name", con=connection)
        else:
            return None

    return result
    
def film_in_category_case_insensitive(category:Union[int,str])->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuł filmu, język, oraz kategorię dla zadanego:
        - id: jeżeli categry jest int
        - name: jeżeli category jest str
    Przykład wynikowej tabeli:
    |   |title          |languge    |category|
    |0	|Amadeus Holy	|English	|Action|
    
    Tabela wynikowa ma być posortowana po tylule filmu i języku.
    
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
    
    Parameters:
    category (int,str): wartość kategorii po id (jeżeli typ int) lub nazwie (jeżeli typ str)  dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(category) == str:
        result = pd.read_sql(f"SELECT film.title, language.name as languge, category.name as category\
                          FROM film \
                          INNER JOIN film_category \
                          ON film.film_id = film_category.film_id \
                          INNER JOIN category \
                          ON film_category.category_id = category.category_id \
                          INNER JOIN language \
                          ON film.language_id = language.language_id \
                          WHERE category.name ILIKE '{category}'\
                          ORDER BY film.title, language.name", con=connection)
    else:
        if type(category) == int:
            result = pd.read_sql(f"SELECT film.title, language.name as languge, category.name as category\
                          FROM film \
                          INNER JOIN film_category \
                          ON film.film_id = film_category.film_id \
                          INNER JOIN category \
                          ON film_category.category_id = category.category_id \
                          INNER JOIN language \
                          ON film.language_id = language.language_id \
                          WHERE category.category_id = {category}\
                          ORDER BY film.title, language.name", con=connection)
        else:
            return None

    return result
    
def film_cast(title:str)->pd.DataFrame:
    ''' Funkcja zwracająca wynik zapytania do bazy o obsadę filmu o dokładnie zadanym tytule.
    Przykład wynikowej tabeli:
    |   |first_name |last_name  |
    |0	|Greg       |Chaplin    | 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.
    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    title (int): wartość id kategorii dla którego wykonujemy zapytanie
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    if type(title) != str:
        return None
        result = pd.read_sql(f" SELECT category.name as category, AVG(film.length), SUM(film.length), MIN(film.length), MAX(film.length)\
                            FROM film \
                            INNER JOIN film_category \
                            ON film.film_id = film_category.film_id \
                            INNER JOIN category \
                            ON film_category.category_id = category.category_id \
                            WHERE category.name = {name} \
                            GROUP BY category.name", con=connection)
    

def film_title_case_insensitive(words:list) :
    ''' Funkcja zwracająca wynik zapytania do bazy o tytuły filmów zawierających conajmniej jedno z podanych słów z listy words.
    Przykład wynikowej tabeli:
    |   |title              |
    |0	|Crystal Breaking 	| 
    
    Tabela wynikowa ma być posortowana po nazwisku i imieniu klienta.

    Jeżeli warunki wejściowe nie są spełnione to funkcja powinna zwracać wartość None.
        
    Parameters:
    words(list): wartość minimalnej długości filmu
    
    Returns:
    pd.DataFrame: DataFrame zawierający wyniki zapytania
    '''
    return None
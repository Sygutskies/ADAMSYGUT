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
    title = "'" + title + "'"

    result = pd.read_sql(f"SELECT film.title, actor.first_name as first_name, actor.last_name as last_name\
    FROM film\
    INNER JOIN film_actor\
    ON film.film_id = film_actor.film_id\
    INNER JOIN actor\
    ON film_actor.actor_id = actor.actor_id\
    INNER JOIN inventory\
    ON film.film_id = inventory.film_id\
    INNER JOIN rental\
    ON inventory.inventory_id = rental.inventory_id\
    INNER JOIN customer\
    ON rental.customer_id = customer.customer_id\
    WHERE film.title = {title}\
    ORDER BY customer.last_name DESC, customer.first_name DESC", con=connection)

    return result


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
    if type(words) != str:
        return None
    words = "'" + words + "'"

    result = pd.read_sql(f"SELECT film.title\
        FROM film\
        INNER JOIN inventory\
        ON film.film_id = inventory.film_id\
        INNER JOIN rental\
        ON inventory.inventory_id = rental.inventory_id\
        INNER JOIN customer\
        ON rental.customer_id = customer.customer_id\
        WHERE film.title ILIKE IN words\
        ORDER BY customer.last_name, customer.first_name", con=connection)

    return result

def country_names_started_from_P():
    result = pd.read_sql(f"SELECT country.country\
    FROM country\
    WHERE LEFT(country.country, 1) ILIKE 'P'", con=connection)

    return result

def country_names_started_from_P_and_end_s():
    result = pd.read_sql(f"SELECT country.country\
    FROM country\
    WHERE LEFT(country.country, 1) ILIKE 'P' AND RIGHT(country.country, 1) ILIKE 's'", con=connection)

    return result

def film_with_number_in_title():
    result = pd.read_sql(f"SELECT film.title\
    FROM film\
    WHERE film.title ILIKE '%[0-9]%'", con=connection)

    return result

def staff_with_dual_first_or_last_name():
    result = pd.read_sql(f"SELECT staff.first_name, staff.last_name\
    FROM staff\
    WHERE staff.first_name ILIKE '% %' OR staff.last_name ILIKE '% %'", con=connection)

    return result

def actors_with_5_char_and_started_from_P_or_S():
    result = pd.read_sql(f"SELECT actor.first_name, actor.last_name\
    FROM actor\
    WHERE LENGTH(actor.last_name) = 5 AND (LEFT(actor.last_name, 1) ILIKE 'P' OR LEFT(actor.last_name, 1) ILIKE 'S')", con=connection)

    return result

def films_with_trip_or_alone():
    result = pd.read_sql(f"SELECT film.title\
    FROM film\
    WHERE film.title ILIKE '%Trip%' OR film.title ILIKE '%Alone%'", con=connection)

    return result

def xd():
    result = pd.read_sql(f"select first_name from actor where first_name ~ '^ Al[a: z, 1: 9] *'", con=connection)
    return result

def xd2():
    result = pd.read_sql(f"select first_name from actor where first_name ~ * '^ al[a: z, 1: 9] *'", con=connection)
    return result
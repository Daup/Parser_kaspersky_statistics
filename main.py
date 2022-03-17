import multiprocessing
import threading
import lxml
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen

# URL_list_of_countries = 'https://statistics.securelist.com/countries'
URL_list_of_countries_ru = 'https://statistics.securelist.com/ru/countries'
# списки для графика
value = []
data = []
country = []
# списки для таблицы угроз
danger = []
rang = []
percent = []


# get_html - функция для получения html
def get_html(url):
    r = urlopen(url=url).read().decode('UTF-8')
    return r


# get_link_countries - функция для получения ссылок стран
def get_link_countries(url=URL_list_of_countries_ru):
    soup = BeautifulSoup(get_html(url), 'lxml')
    list_table = soup.find_all(name='a', class_='card')
    list_link_countries = []
    list_link_countries_month = []

    for item in list_table:
        list_link_countries.append(item.get('href'))

    for item in list_link_countries:
        i = item[:-3] + 'month'
        list_link_countries_month.append(i)

    return list_link_countries_month


# get_country - функция для получения названия страны
def get_country(url):
    countries = str(url)
    countries = countries.replace('https://statistics.securelist.com/ru/country/', '')
    countries = countries.replace('/on-access-scan/month', '')
    countries = countries.replace('%20', '_')
    countries = countries.replace('/intrusion-detection-scan/month', '')
    return countries


# get_value_table - функция для получения значения таблицы (значения будут получены, как один единый список)
def get_value_table(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    table = soup.find(name='div', class_='container', id='chart-wrapper')
    meaning = table.get('data-chart')
    meaning = meaning.replace('[', '')
    meaning = meaning.replace(']', '')
    meaning = meaning.replace('{', '')
    meaning = meaning.replace('}', '')
    meaning = meaning.replace('"', '')
    meaning = meaning.replace('value:', '')
    meaning = meaning.replace('date:', '')
    meaning = meaning.split(',')
    return meaning


# get_danger_table - функция для получения значений (топ-10 обнаруженных угроз)
def get_danger_table(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    table = soup.find_all(name='div', class_='p-2 d-flex align-items-center')
    danger = []
    rang = []
    percent = []
    for item in table:
        danger += item.find(name='span')
        percent += item.find(name='div', class_='ml-auto list-value')
    count = len(danger)
    country = [get_country(url)]
    if count != 0:
        for i in range(1, count + 1):
            rang.append(i)
    else:
        danger = ['nope']
        rang = ['0']
        percent = ['0']

    return rang, danger, percent, country


# len_data - функция счета размеров таблицы для каждой страны
def len_data():
    number = 0
    list_data = get_link_countries()
    for item in list_data:
        print('index: {} len {}'.format(number, len(get_value_table(item))))
        number += 1


# work_with_xlsx - функция для получения value, data, country
# item - список полученный в функции  get_value_table
def work_with_xlsx(item, url):
    len_item = len(item)
    country = [get_country(url)]
    if len_item == 1:
        value = ['no data']
        data = ['2000-01-01 00:00']
    else:
        value = item[::2]
        data = item[1::2]

    return value, data, country


def main_def(urls):
    item = get_value_table(urls)
    value_i, data_i, country_i = work_with_xlsx(item, urls)
    return value_i, data_i, country_i


# end_def - функция записи значений в список value, data, country
def end_def(response):
    global value, data, country
    count = []
    for i in response[0]:
        value.append(i)
        count.append(i)
    for i in response[1]:
        data.append(i)
    country += response[2] * len(count)


def end_danger_def(response):
    global rang, danger, percent, country
    count = []
    print(response)
    for i in response[0]:
        danger.append(i)
        count.append(i)
    for i in range(1, len(count) + 1):
        rang.append(i)
    for i in response[1]:
        percent.append(i)
    country += response[2] * len(count)

if __name__ == '__main__':
    work = 0
    if work == 1:
        list = get_link_countries()
        df = pd.read_excel('D:\Project\parserForNirs\dataframe.xlsx')

        with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as p:
            for i in list:
                p.apply_async(main_def, args=(i,), callback=end_def)
            p.close()
            p.join()

        df = pd.DataFrame({'value': value, 'data': data, 'country': country})
        df.to_excel('D:\Project\parserForNirs\dataframe.xlsx', index=False)
    else:
        list = get_link_countries()
        print(list)
        df = pd.read_excel('D:\Project\parserForNirs\dataframe_danger.xlsx')
        #with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as p:
        #    for i in list:
        #        p.apply_async(get_danger_table, args=(i,), callback=end_danger_def)
        #    p.close()
        #    p.join()
        for i in list:
            rang_i, danger_i, percent_i, country_i = get_danger_table(i)
            for i in rang_i:
                rang.append(i)
            for i in danger_i:
                danger.append(i)
            for i in percent_i:
                percent.append(i)
            country += country_i * len(danger_i)

        df = pd.DataFrame({'rang': rang, 'name_danger': danger, 'percent': percent, 'country': country})
        df.to_excel('D:\Project\parserForNirs\dataframe_danger.xlsx', index=False)


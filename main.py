import multiprocessing
import threading
import lxml
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen


# URL_list_of_countries = 'https://statistics.securelist.com/countries'
URL_list_of_countries_ru = 'https://statistics.securelist.com/ru/countries'
value = []
data = []
country = []


def get_html(url):
    r = urlopen(url=url).read().decode('UTF-8')
    return r


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


def get_country(url):
    countries = str(url)
    countries = countries.replace('https://statistics.securelist.com/ru/country/', '')
    countries = countries.replace('/on-access-scan/month', '')
    countries = countries.replace('%20', '_')
    countries = countries.replace('/intrusion-detection-scan/month', '')
    return countries


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


def len_data():
    number = 0
    list_data = get_link_countries()
    for item in list_data:
        print('index: {} len {}'.format(number, len(get_value_table(item))))
        number += 1


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


def end_def(response):
    global value, data, country
    count = []
    for i in response[0]:
        value.append(i)
        count.append(i)
    for i in response[1]:
        data.append(i)
    country += response[2] * len(count)


if __name__ == '__main__':
    list = get_link_countries()
    df = pd.read_excel('D:\Project\parserForNirs\dataframe.xlsx')

    with multiprocessing.Pool(multiprocessing.cpu_count() * 3) as p:
        for i in list:
            p.apply_async(main_def, args=(i,), callback=end_def)
        p.close()
        p.join()

    df = pd.DataFrame({'value': value, 'data': data, 'country': country})
    df.to_excel('D:\Project\parserForNirs\dataframe.xlsx', index=False)

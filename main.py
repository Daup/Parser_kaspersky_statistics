# import multiprocessing
# import threading
import lxml
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from urllib.request import urlopen

URL_list_of_countries = 'https://statistics.securelist.com/countries'
URL_list_of_countries_ru = 'https://statistics.securelist.com/ru/countries'


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
    country = str(url)
    country = country.replace('https://statistics.securelist.com/ru/country/', '')
    country = country.replace('/on-access-scan/month', '')
    country = country.replace('%20', '_')
    country = country.replace('/intrusion-detection-scan/month', '')
    return country


def get_value_table(url):
    soup = BeautifulSoup(get_html(url), 'lxml')
    table = soup.find(name='div', class_='container', id='chart-wrapper')
    value = table.get('data-chart')
    value = value.replace('[', '')
    value = value.replace(']', '')
    value = value.replace('{', '')
    value = value.replace('}', '')
    value = value.replace('"', '')
    value = value.replace('value:', '')
    value = value.replace('date:', '')
    value = value.split(',')
    return value


def len_data():  # list = get_link_countries()
    number = 0
    list_data = get_link_countries()
    for i in list_data:
        print('index: {} len {}'.format(number, len(get_value_table(i))))
        number += 1


def work_with_xlsx(item, url):
    len_item = len(item)
    country = [get_country(url)]
    if len_item == 1:
        value = ['нет данных']
        data = ['2000-01-01 00:00']
        # df = pd.DataFrame({'value': value, 'data': data,'country': country})
    else:
        value = item[::2]
        data = item[1::2]
        # df = pd.DataFrame({'value': value, 'data': data, 'country': country})
    return value, data, country


if __name__ == '__main__':
    list = get_link_countries()
    df = pd.read_excel('D:\Project\parserForNirs\dataframe.xlsx')

    value = []
    data = []
    country = []

    for urls in list:
        item = get_value_table(urls)
        value_i, data_i, country_i = work_with_xlsx(item, urls)
        value += value_i
        data += data_i
        country += country_i * len(value_i)

    df = pd.DataFrame({'value': value, 'data': data, 'country': country})  # , 'country': country
    df.to_excel('D:\Project\parserForNirs\dataframe.xlsx', index=False)

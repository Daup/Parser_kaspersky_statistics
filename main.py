import lxml
import pika
import asyncio
import json
from elasticconnector import ElasticConnector
from bs4 import BeautifulSoup
from urllib.request import urlopen
from natasha import (Segmenter,MorphVocab,NewsEmbedding,NewsMorphTagger,NewsSyntaxParser,NewsNERTagger,PER,NamesExtractor,Doc,DatesExtractor)


class IpiadHabr:

    def __init__(self):
        self.url_head = 'https://habr.com/ru/news/page1/'
        self.list_author = []
        self.list_link = []
        self.list_heading = []
        self.list_data = []
        self.list_text = []
        self.list_page_link = []
        self.range_page = 0

    def __str__(self):
        return f'url_head: {self.url_head} \n' \
               f'list_author: {self.list_author} \n' \
               f'list_link: {self.list_link} \n' \
               f'list_heading: {self.list_heading} \n' \
               f'list_data: {self.list_data} \n' \
               f'list_text: {self.list_text} \n' \
               f'list_page_link: {self.list_page_link} \n' \
               f'range page: {self.range_page} \n'

    @staticmethod
    def get_html(url):
        r = urlopen(url=url).read().decode('UTF-8')
        return r

    def get_range_page(self):
        soup = BeautifulSoup(self.get_html(self.url_head), 'lxml')
        range_page = soup.find(name='div', class_='tm-pagination__pages')
        list = []
        for item in range_page.select('a'):
            item = int(item.get_text())
            list.append(item)
        self.range_page = max(list)

        for item in range(1, self.range_page + 1):
            link = f'https://habr.com/ru/news/page{item}/'
            self.list_page_link.append(link)

        return self.range_page

    async def get_author(self, link):
        soup = BeautifulSoup(self.get_html(link), 'lxml')
        author = soup.find_all(name='a', class_='tm-user-info__username')
        for item in author:
            item = item.get_text().strip()
            self.list_author.append(item)
        return self.list_author

    async def get_link_and_heading(self, link):
        # soup = BeautifulSoup(self.get_html(self.url_head), 'lxml')
        soup = BeautifulSoup(self.get_html(link), 'lxml')
        link = soup.find_all(name='a', class_='tm-article-snippet__title-link')
        for item in link:
            item = f"{item['href']}"
            item = f'https://habr.com{item}'
            self.list_link.append(item)

        for item in link:
            self.list_heading.append(item.get_text())

    async def get_data(self, link):
        # soup = BeautifulSoup(self.get_html(self.url_head), 'lxml')
        soup = BeautifulSoup(self.get_html(link), 'lxml')
        data = soup.find_all(name='time')
        for item in data:
            item = item.get_text()
            self.list_data.append(item)

    async def get_text(self, link):
        soup = BeautifulSoup(self.get_html(link), 'lxml')
        text = soup.find_all(name='p')
        for item in text:
            item = item.get_text()
            self.list_text.append(item)

    async def func_main(self, list_page_link):
        chunk = 5
        tasks = []
        for item in list_page_link:
            # await self.get_author(item)
            # await self.get_link_and_heading(item)
            # await self.get_data(item)
            tasks.append(asyncio.create_task(self.get_author(item)))
            tasks.append(asyncio.create_task(self.get_link_and_heading(item)))
            tasks.append(asyncio.create_task(self.get_data(item)))
            if len(tasks) == chunk:
                await asyncio.gather(*tasks)
                tasks = []
            # await asyncio.gather(task_author,task_link,task_data)
            # self.list_page_link.remove(item)

    async def func_main_text(self, list_link):
        chunk = 5
        tasks = []
        for item in list_link:
            tasks.append(asyncio.create_task(self.get_text(item)))
            if len(tasks) == chunk:
                await asyncio.gather(*tasks)
                tasks = []

    def identification(self, text):
        segmenter = Segmenter()
        emb = NewsEmbedding()
        morph_tagger = NewsMorphTagger(emb)
        syntax_parser = NewsSyntaxParser(emb)
        # ner_tagger = NewsNERTagger(emb)

        doc = Doc(text)
        doc.segment(segmenter)
        doc.tag_morph(morph_tagger)
        doc.parse_syntax(syntax_parser)
        # doc.tag_ner(ner_tagger)
        return doc.tokens[:3]
        # return doc.sents[:5]


if __name__ == '__main__':
    news = IpiadHabr()
    news.get_range_page()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(news.func_main(list_page_link=news.list_page_link))
    loop.run_until_complete(news.func_main_text(list_link=news.list_link))

    list_json = json.dumps([news.list_author, news.list_link, news.list_heading, news.list_data, news.list_text])
    credentials = pika.PlainCredentials('guest', 'guest')
    parameters = pika.ConnectionParameters('localhost', 5672, '/', credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='link')
    channel.basic_publish(exchange='',
                          routing_key='link',
                          body=list_json)
    

    def RecvMsg():

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672,
                                                                       credentials=pika.PlainCredentials('guest',
                                                                                                         'guest')))

        def callback(ch, method, properties, body):
            print("Received[*] ")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            jsonobj = json.loads(body.decode("utf-8"))
            print(jsonobj)
            array1 = jsonobj[0]
            array2 = jsonobj[1]
            array3 = jsonobj[2]
            array4 = jsonobj[3]
            array5 = jsonobj[4]
            es = ElasticConnector()

            for item in range(0, len(jsonobj[0])):
                json_item = {'author': array1[item],
                             'link': array2[item],
                             'heading': array3[item],
                             'data': array4[item],
                             'text': array5[item]}
                es.AppendNew(json_item)
            return jsonobj

        channel = connection.channel()
        channel.queue_declare(queue='link')
        channel.basic_consume(on_message_callback=callback, queue="link")
        channel.start_consuming()

    RecvMsg()

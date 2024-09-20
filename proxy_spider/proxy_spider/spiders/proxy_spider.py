import random
import scrapy
import re
import base64
import json
import datetime

from functools import partial
from proxy_spider.settings import TOKEN
from proxy_spider.user_agents import USER_AGENTS


class FreeProxySpider(scrapy.Spider):
    name = 'free_proxy_spider'
    start_urls = ['http://free-proxy.cz/en/']

    def __init__(self, *args, **kwargs):
        super(FreeProxySpider, self).__init__(*args, **kwargs)
        self.page_count = 0
        self.http_proxies = []
        self.all_proxies = []
        self.token = TOKEN
        self.form_token = ''
        self.result = {}
        self.start_time = datetime.datetime.now()
        self.chunk_size = 10
        self.chunks = []
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-GB;q=0.8,en;q=0.7,en-US;q=0.6',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Host': 'free-proxy.cz',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': random.choice(USER_AGENTS),
        }

    def start_requests(self):
        """ Устанавливает заголовки в первый запрос. """
        for url in self.start_urls:
            yield scrapy.Request(url, headers=self.headers)

    def parse(self, response, **kwargs):
        rows = response.css('table#proxy_list tbody tr')
        for row in rows:
            script_content = row.xpath('.//script[contains(text(),"Base64.decode")]/text()').get()
            if not script_content:
                continue
            string = re.search(r'Base64\.decode\("([^"]+)"\)', script_content).group(1)
            proxy_ip = base64.b64decode(string).decode('utf-8')
            proxy_port = row.css('span.fport::text').get()
            proxy_type = row.css('td small::text').get()

            if proxy_ip and proxy_port:
                proxy = f"{proxy_ip}:{proxy_port}"
                self.all_proxies.append(proxy)
                if proxy_type.lower() in ['http', 'https']:
                    self.http_proxies.append(proxy)

        self.page_count += 1
        if self.page_count < 5:
            next_page = response.css('div.paginator a::attr(href)').getall()[-1]
            if next_page:
                yield response.follow(next_page, self.parse, headers=self.headers)
        else:
            if self.all_proxies:
                self.chunks = [self.all_proxies[i:i + self.chunk_size] for i in
                               range(0, len(self.all_proxies), self.chunk_size)]

                for chunk in self.chunks:
                    if len(chunk) < 3:
                        continue
                    yield self.get_request(chunk)

            else:
                self.logger.info(f"Нет адресов.")

    def get_request(self, chunk):
        proxy = f'http://{random.choice(self.http_proxies)}'
        self.logger.info(f"Отправляется запрос, proxy: {proxy}")
        return scrapy.Request(
            url='https://test-rg8.ddns.net/api/get_token',
            method='GET',
            callback=self.get_form_token,
            meta={'proxy': proxy},
            cb_kwargs={'chunk': chunk},
            errback=partial(self.handle_error, chunk=chunk),
            dont_filter=True,
        )

    def get_form_token(self, response, chunk):
        cookie = response.headers.get('set-cookie')
        self.logger.info(f"Cookie - {cookie}")
        if not cookie:
            self.get_request(chunk)
        else:
            proxy = response.request.meta['proxy']
            self.logger.info(f"proxy - {proxy}")

            json_data = {
                'user_id': self.token,
                'len': len(chunk),
                'proxies': ', '.join(chunk)
            }

            return scrapy.Request(
                url='https://test-rg8.ddns.net/api/post_proxies',
                method='POST',
                body=json.dumps(json_data),
                callback=self.get_save_id,
                meta={'proxy': proxy},
                cb_kwargs={'chunk': chunk},
                dont_filter=True,
                errback=partial(self.handle_error, chunk=chunk),
                headers={
                    'User-Agent': random.choice(USER_AGENTS),
                    'Content-Type': 'application/json',
                    'Referer': 'https://test-rg8.ddns.net/task',
                    'Cookie': f'{cookie}'
                },
            )

    def handle_error(self, failure, chunk):
        yield self.get_request(chunk)

    def get_save_id(self, response, chunk):
        save_id = response.json().get('save_id')
        self.logger.info(f"save_id - {save_id}")
        if save_id:
            self.result[save_id] = chunk
            if len(self.chunks) == len(self.result):
                self.save_results()
        else:
            yield from self.get_request(chunk)

    def save_results(self):
        """ Сохраняет результаты. """
        with open('results.json', 'w') as f:
            json.dump(self.result, f)

        execution_time = str(datetime.datetime.now() - self.start_time).split('.')[0]
        with open('time.txt', 'w') as f:
            f.write(execution_time)

        self.logger.info(f"Результаты сохранены. Время выполнения: {execution_time}")

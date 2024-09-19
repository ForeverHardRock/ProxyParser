import random
import scrapy
import time
import re
import base64
import requests
import json
import datetime
from proxy_spider.settings import TOKEN
from proxy_spider.user_agents import USER_AGENTS


class FreeProxySpider(scrapy.Spider):
    name = 'free_proxy_spider'
    start_urls = ['http://free-proxy.cz/en/']

    def __init__(self, *args, **kwargs):
        super(FreeProxySpider, self).__init__(*args, **kwargs)
        self.page_count = 0
        self.all_proxies = []
        self.token = TOKEN
        self.form_token = ''
        self.result = {}
        self.start_time = datetime.datetime.now()
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
            encoded_ip = row.xpath('//script[contains(text(),"Base64.decode")]/text()').get()
            string = re.search(r'Base64\.decode\("([^"]+)"\)', encoded_ip).group(1)
            proxy_ip = base64.b64decode(string).decode('utf-8')
            proxy_port = row.css('span.fport::text').get()

            if proxy_ip and proxy_port:
                proxy = f"{proxy_ip}:{proxy_port}"
                self.all_proxies.append(proxy)

        self.page_count += 1
        if self.page_count < 5:
            next_page = response.css('div.paginator a::attr(href)').getall()[-1]
            if next_page:
                yield response.follow(next_page, self.parse, headers=self.headers)
        else:
            if self.all_proxies:
                self.upload_proxies()
            else:
                self.logger.info(f"Нет адресов.")

    def upload_proxies(self):
        """ Отправляет полученные прокси в форму чанками по 10 штук
        и получает save_id для каждого чанка. """
        while len(self.all_proxies) > 2:
            cookie = requests.get('https://test-rg8.ddns.net/api/get_token').headers.get('set-cookie')
            proxies = [self.all_proxies.pop(0) for _ in range(min(10, len(self.all_proxies)))]
            json_data = {
                'user_id': self.token,
                'len': len(proxies),
                'proxies': ', '.join(proxies)
            }

            response = requests.post(
                url='https://test-rg8.ddns.net/api/post_proxies',
                headers={
                    'User-Agent': random.choice(USER_AGENTS),
                    'Content-Type': 'application/json',
                    'Referer': 'https://test-rg8.ddns.net/task',
                    'Cookie': f'{cookie}'
                },
                data=json.dumps(json_data),
            )
            json_response = response.json()
            save_id = json_response.get('save_id')
            if save_id:
                self.result[save_id] = proxies
            else:
                self.all_proxies += proxies
            if len(self.all_proxies) > 2:
                time.sleep(random.randint(5, 10))

        self.save_results()

    def save_results(self):
        """ Сохраняет результаты. """
        with open('results.json', 'w') as f:
            json.dump(self.result, f)

        execution_time = str(datetime.datetime.now() - self.start_time).split('.')[0]
        with open('time.txt', 'w') as f:
            f.write(execution_time)

        self.logger.info(f"Результаты сохранены. Время выполнения: {execution_time}")

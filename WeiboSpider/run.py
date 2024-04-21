import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.comment import CommentSpider
from spiders.user import UserSpider
from spiders.fan import FanSpider

if __name__ == '__main__':
    mode = sys.argv[1]
    os.environ['SCRAPY_SETTINGS_MODULE'] = 'settings'
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    mode_to_spider = {
        'comment': CommentSpider,
        'fan': FanSpider,
        'user': UserSpider,
    }
    process.crawl(mode_to_spider[mode])
    # the script will block here until the crawling is finished
    process.start()
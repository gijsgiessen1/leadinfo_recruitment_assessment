from urllib.request import Request
from wsgiref.util import request_uri
from requests import request
import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet.error import DNSLookupError
import os
import sys
import pandas as pd

## This script was written by Gijs van der Giessen as an assessment for possible future employment at the company Leadinfo ##





# This function gets the path to the parent directory so that the CSV file with the domains can be read from there
def get_parent_directory():

    absolutepath = os.path.abspath(__file__)
    fileDirectory = os.path.dirname(absolutepath)
    parentDirectory = os.path.dirname(fileDirectory)
    sys.path.append(parentDirectory)
    return parentDirectory

# This function reads the domain names out of the CSV file and puts them in a list
def create_list_of_domains():
    domains = pd.read_csv(get_parent_directory() + "\domain-list.csv", header=None)
    list_of_domains = domains.values.tolist()
    return list_of_domains

# This function converts the domain names into urls that are readable for Scrapy 
def create_list_of_urls():
    list_of_urls = []
    list_of_domains = create_list_of_domains()

    for domain in list_of_domains:
        url = "http://"+domain[0]+"/"
        list_of_urls.append(url)
    return list_of_urls
    

# This is the crawler class from which class methods are called in order to run the program
class BusinesscrawlerSpider(scrapy.Spider):
    name = 'businesscrawler'
    allowed_domains = create_list_of_domains()
    start_urls = create_list_of_urls()

    # we are running 100 concurrent requests in order to speed up the program
    custom_settings = {
        'CONCURRENT_REQUESTS': 100,
    }
    
    # It is customary for a Scrapy webcrawler to have a parse method, but in this case it is empty
    def parse(self, response):
        pass

    # Instead we immediately start doing requests    
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse_info,
                errback=self.errback_httpbin,
                meta={
                    'handle_httpstatus_all': True,
                    'dont_retry': True,
                },
            )

    # This method parses the domain urls and HTTP response status in the default case and gives them back through calling the yield keyword
    def parse_info(self, response):
        yield {
            'domain': response.url,
            'status': response.status,
        }

    # This method makes sure the program catches DNSLookupErrors and then again gives back the domain url and the status (DNSLookupError in this case)
    def errback_httpbin(self,failure):
        if failure.check(DNSLookupError):
            
            yield{
                'domain': failure.request.url,
                'status': 'DNSLookupError',
            }
            

#The CrawlerProcess function call initializes the process and makes sure the results are captured in a CSV file called Business_website_responses.csv
process = CrawlerProcess(settings={
    "FEEDS": {
        "Business_website_responses.csv": {"format": "csv"},
    },
})
# The last two function calls start the crawler.
process.crawl(BusinesscrawlerSpider)
process.start()
   
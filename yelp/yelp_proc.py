from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait # available since 2.4.0
from selenium.webdriver.support import expected_conditions as EC # available since 2.26.0
from selenium.common.exceptions import TimeoutException
from selenium.common import exceptions as EX
from multiprocessing.pool import ThreadPool, Pool
import sys
import threading
import argparse
import time
import signal
from bs4 import BeautifulSoup
import random
import json
from urllib.parse import unquote
from urllib.parse import urlparse
from lxml import etree
from lxml import html
import csv
import re
from re import compile, IGNORECASE, findall
import requests


RATING_THRESHOLD = 4.5

threadLocal = threading.local()


class ProgramKilled(Exception):
    pass


def signal_handler(signum, frame):
    raise ProgramKilled


def wait_for(min, max):
    time.sleep(random.uniform(min, max))


def get_substring(instr, startstr, endstr=None):
    """
    Get sub string between the start and end string
    """
    if startstr == "":
        length = instr.find(endstr)
        if length >= 0:
            return instr[:length]

    start_idx = instr.find(startstr)
    if start_idx == -1:
        return ""
    start_idx += len(startstr)
    if start_idx >= 0:
        if endstr:
            length = instr[start_idx:].find(endstr)
            if length == -1:
                return instr[start_idx:]
            return instr[start_idx:start_idx+length]
        return instr[start_idx:]


def get_business_website_url(driver, params):

    page_urls = params['urls']
    throttle = params['throttle']
    output_file = params['output']
    verbose = params['verbose']

    for page_url in page_urls:
        try:
            driver.get(page_url)
        except:
            continue
        try:
            WebDriverWait(driver, 100).until(EC.presence_of_element_located((By.XPATH, '//div[contains(@class, "main-content-wrap")]')))
        except TimeoutException:
            print("* page_url: ",page_url)
            continue

        soup = BeautifulSoup(driver.page_source, 'lxml')
        tree = etree.HTML(driver.page_source)

        jsonItems = soup.findAll("script", {"type": "application/ld+json"})
        if len(jsonItems) == 0:
            wait_for(throttle - 0.5, throttle + 0.5)
            continue

        bizItem = jsonItems[-1]
        jsonTxt = bizItem.text.strip()
        bizObj = json.loads(jsonTxt)

        ratingValue = 5.0
        if 'aggregateRating' in bizObj:
            if 'ratingValue' in bizObj['aggregateRating']:
                ratingValue = float(bizObj['aggregateRating']['ratingValue'])

        if verbose == True:
            print("Rating Value :", ratingValue, page_url)

        if ratingValue > RATING_THRESHOLD:
            wait_for(throttle - 0.5, throttle + 0.5)
            continue

        # parsing info
        yelp_rating = ratingValue
        business_name = soup.find('div', {'class': 'hidden'}).find(
            'meta', {'itemprop': 'name'})['content']
        price_range = soup.find('div', {'class': 'hidden'}).find(
            'meta', {'itemprop': 'priceRange'})['content']
        category = ', '.join(tree.xpath('//a[contains(@href, "/c/")]/text()'))

        try:
            phone = soup.find('span', {'itemprop': 'telephone'}).text.strip()
        except:
            phone = ''

        address_ = []
        try:
            streetAddress = soup.find(
                'span', {'itemprop': 'streetAddress'}).text.strip()
        except:
            streetAddress = ''
        address_.append(streetAddress)
        try:
            addressLocality = soup.find(
                'span', {'itemprop': 'addressLocality'}).text.strip()
        except:
            addressLocality = ''
        address_.append(addressLocality)

        try:
            region = soup.find(
                'span', {'itemprop': 'addressRegion'}).text.strip()
        except:
            region = ''
        address_.append(region)
        try:
            postalCode = soup.find(
                'span', {'itemprop': 'postalCode'}).text.strip()
        except:
            postalCode = ''
        address_.append(postalCode)
        address = ' '.join(address_).strip()
        try:
            reviews = soup.find(
                'span', {'itemprop': 'reviewCount'}).text.strip()
        except:
            reviews = ''
        claimed_tag = tree.xpath(
            '//span[contains(@class, "checkmark-badged")]')

        if len(claimed_tag) > 0:
            claimed = True
        else:
            claimed = False

        website_url = ""
        biz_website_link = soup.find('a', {'rel': 'noopener'})
        emails = []
        if biz_website_link:
            website_url = biz_website_link['href']
            website_url = get_substring(website_url, "url=", "&website")
            if website_url != "":
                website_url = unquote(website_url)
                print("Business website :", website_url)
                emails = EmailScraper(website_url).extract_mail_add()
                print('Found these email address(es) :', emails)

        # write csv
        if len(emails) > 0:
            for e in emails:
                my_data = [business_name, category, yelp_rating, price_range,
                           phone, address, e, claimed, reviews, website_url, page_url]
                writer = csv.writer(output_file)
                writer.writerow(my_data)
        else:
            my_data = [business_name, category, yelp_rating, price_range,
                        phone, address, '', claimed, reviews, website_url, page_url]
            writer = csv.writer(output_file)
            writer.writerow(my_data)            

        wait_for(throttle - 0.5, throttle + 0.5)


def get_driver(debug):

    driver = getattr(threadLocal, 'driver', None)

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches",["ignore-certificate-errors"])
    options.add_argument('--disable-gpu')
    options.add_argument('--headless')

    if driver is None:
        # driver = webdriver.Chrome("chromedriver.exe", chrome_options=options)
        if debug:
            driver = webdriver.Chrome()
            driver.set_window_size(0, 0)
        else:
            driver = webdriver.Chrome(chrome_options=options)
        setattr(threadLocal, 'driver', driver)
        return driver

def thread_proc(params):

    debug = params['debug']

    driver = get_driver(debug)
    get_business_website_url(driver, params)
    driver.quit()


def divide_chunks(urls, n):
    total_count = len(urls)
    chunk_size = int(total_count / n) + 1

    for i in range(0, total_count, chunk_size):
        yield urls[i:i + chunk_size]

class EmailScraper(object):
    # Email scraper

    def __init__(self, domain_name=None):
        self.visited = {'/'}
        self.extracted_mail = []
        self.domain_name = domain_name
        self.dn_as_list = domain_name.split('.')[1:]
        self.cnt = 0

    def extract_mail_add(self):
        node_list = ['{}'.format(self.domain_name)]
        result = []

        while node_list:
            current_node = node_list.pop()
            if current_node not in self.visited:
                self.visited.add(current_node)
                try:
                    r = get_html(current_node)
                except:
                    r = None
                if r:
                    if len(node_list) < 50:
                        node_list.extend(fetch_links(r['html_lxml'], self.domain_name, self.dn_as_list, self.visited))
                    for address in find_mail_address(r['html']):
                        if address.lower() not in self.extracted_mail:
                            self.extracted_mail.append(address.lower())
                self.cnt = self.cnt + 1
            if self.cnt == 30:
                break
        if self.extracted_mail:
            for address in self.extracted_mail:
                if check_spamtxt(address):
                    result.append(address)
        else:
            print('Could not find an email-address on {}!'.format(self.domain_name))

        return result
        
def check_spamtxt(address):
    # check spam text
    global spamtraps

    for spamtrap in spamtraps:
        if spamtrap.lower() in address.lower():
            return False

    if is_valid(address):
        return True

def is_valid(email):

    # check email validation

    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    if(re.search(regex,email)):
        return True
    else:
        return False


def get_html(link=None):
    # get page source

    if link:
        result = {}
        headers = {
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        }
        r = requests.get(link, headers=headers, timeout=60)
        result['html'] = r.text
        result['html_lxml'] = BeautifulSoup(r.text, "lxml")

        return result


def in_same_domain(source=None, target=None):
    # check domain status

    if all([type(source) == list, type(target) == str]):
        return source == urlparse(target).netloc.split('.')[-len(source):]
    return False


def find_mail_address(html_corpus=None):
    # find email address

    emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]{2,3}", html_corpus)
    result = []
    for email in emails:
        if email.lower() not in result:
            result.append(email.lower())

    return result


def fetch_links(html_corpus=None, domain_name=None, dn_as_list=None, visited_links=None):
    # get links at page
    
    links = []
    if all([html_corpus, domain_name, type(visited_links) == set, type(dn_as_list) == list]):
        for link in html_corpus.find_all('a'):
            link = link.get('href', None)
            if link and not link.startswith('#'):
                if link.startswith('/'):
                    to_traverse = "{}{}".format(domain_name, link)
                    if to_traverse not in visited_links:
                        links.append(to_traverse)
                elif '.htm' in link and 'http' not in link:
                    to_traverse = "{}{}".format(domain_name, link)
                    if to_traverse not in visited_links:
                        links.append(to_traverse)                        
                elif in_same_domain(dn_as_list, link) and link not in visited_links:
                    links.append(link)
    return links

if __name__ == "__main__":

    """
    Command(product) : python yelp_proc.py -i input.txt -o output.csv -t 3 -s 1 -v
    Command(debug) : python yelp_proc.py -i input.txt -o output.csv -t 3 -s 1 -v -d
    """

    parser = argparse.ArgumentParser(description='Extracts business website urls which have rating value \
        less than 3.5 from the input plain text file. Format: \n\tpython yelp_proc.py -i <input file name> -o <output file name> -t <thread count> -s <throttle seconds> -v -d')
    parser.add_argument('-i', '--input', type=str,
                        help='input file name(in plain text format)')
    parser.add_argument('-o', '--output', type=str,
                        help='output file name(wesite url in each row)')
    parser.add_argument('-t', '--threads', type=int,
                        help='thread count. must be less than 5')
    parser.add_argument('-s', '--secondrate', type=int,
                        help='throttle time between requests in seconds')
    parser.add_argument('-v', '--verbose',
                        action='store_true', help='verbose mode')
    parser.add_argument('-d', '--debug',
                         action='store_true', help='debug mode')
    args = parser.parse_args()

    if (args.input is None or args.output is None):
        print("input/output parameter must be given!")
        parser.print_help()
        sys.exit(0)

    threads = 1
    if args.threads:
        threads = int(args.threads)
        if threads > 5:
            threads = 5
        if threads < 1:
            threads = 1

    throttle = 1
    if args.secondrate:
        throttle = int(args.secondrate)
        if throttle < 1:
            throttle = 1

    verbose = False
    if args.verbose:
        verbose = True

    debug = False
    if args.debug:
        debug = True

    if verbose:
        print("Parameter", args.input, args.output, threads, throttle, verbose, debug)

    global spamtraps
    with open("spamtraps.txt", "r", encoding='utf-8') as search_f:
        spamtraps = [line.strip() for line in search_f.readlines()]

    # signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    with open(args.input, "r", encoding='utf-8') as fInput:
        with open(args.output, "w", encoding='utf-8', newline='') as fOutput:

            # write csv header
            myFields = ['Business Name', 'Category', 'Yelp Rating', 'Price Range', 'Phone Number',
                        'Address', 'Email', 'Claimed', 'Number of Reviews', 'Website URL', 'Yelp URL']
            writer = csv.writer(fOutput)
            writer.writerow(myFields)

            # read urls from the input file
            url_list = [line.strip() for line in fInput.readlines()]
            if verbose:
                print("Total count =", len(url_list))

            # make parameters for the thread
            params = []
            url_chunks = list(divide_chunks(url_list, threads))
            for i in range(threads):
                param = {
                    "urls": url_chunks[i],
                    "throttle": throttle,
                    "output": fOutput,
                    "verbose": verbose,
                    "debug": debug
                }
                params.append(param)

            # run threads to process yelp pages
            try:
                pool = ThreadPool(threads)
                result = pool.map(thread_proc, params)

                pool.close()
                pool.join()

            except ProgramKilled:
                pool.close()
                pool.join()
                exit(0)

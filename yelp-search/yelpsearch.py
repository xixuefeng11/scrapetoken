from selenium import webdriver
from selenium.common.exceptions import TimeoutException
import sys
import argparse
from argparse import RawTextHelpFormatter
import time, signal
from bs4 import BeautifulSoup
import random
from urllib.parse import quote, unquote


   
class ProgramKilled(Exception):
    pass
class InvalidFileError(Exception):
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
    
    
def get_driver(debug):
    # option setting headless
    
    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches",["ignore-certificate-errors"])
    options.add_argument('--disable-gpu')
    options.add_argument('--headless')
    if debug:
        driver = webdriver.Chrome()
    else:
        # driver = webdriver.Chrome("chromedriver.exe", chrome_options=options)
        driver = webdriver.Chrome(chrome_options=options)

    return driver
    

def parse_argument():
    """
    Command(product) : python script.py -s categories.txt -l city.txt -o output.txt -p 10 -ta 5-10 -tb 2-5 -b 900 -v
    Command(debug) : python script.py -s categories.txt -l city.txt -o output.txt -p 10 -ta 5-10 -tb 2-5 -b 900 -v -d
    """
    
    description = """
Scrape Yelp for the redirect URL of up to a specified number of pages for a search footprint which is provided as an input file.

Syntax:
    python script.py -s <categories_file> -l <city_file> -o <output_file> -p <page_count> -ta <throttling_value_range_1> -tb <throttling_value_range_2> -b <waiting_time_when_banned> -v -d
    
Samples:
    python script.py -s categories.txt -l city.txt -o output.txt -p 10 -ta 5-10 -tb 2-5 -b 900 -v
    python script.py -s categories.txt -l city.txt -o output.txt
    """
    
    parser = argparse.ArgumentParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument('-s', '--search', type=str, help='categories file name(in plain text format), required', required=True)
    parser.add_argument('-l', '--loc', type=str, help='city file name(in plain text format), required', required=True)
    parser.add_argument('-o', '--output', type=str, help='output file name(wesite url in each row), required', required=True)
    parser.add_argument('-p', '--pages', type=int, help='page count. default: 10', default=10)    
    parser.add_argument('-ta', '--throttlea', type=str, help='throttle time range in seconds. default: 5-10', default="5-10")
    parser.add_argument('-tb', '--throttleb', type=str, help='throttle time range in seconds. default: 2-5', default="2-5")
    parser.add_argument('-b', '--banned', type=int, help='waiting time in seconds when banned. default: 900', default=900)
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose mode')
    parser.add_argument('-d', '--debug', action='store_true', help='debug mode')
    
    try:
        args = parser.parse_args()
    except:
        sys.exit(0)
    
    search  = args.search
    loc     = args.loc
    output  = args.output
    
    pages = 10
    if args.pages:
        pages = args.pages    

    ta = [5, 10]
    if args.throttlea:
        throttlea = args.throttlea.split('-')
        if len(throttlea) == 2:
            min = int(throttlea[0])
            max = int(throttlea[1])
            if min > 0 and max > 0 and min < max:
                ta = [min, max]            

    tb = [2, 5]
    if args.throttleb:
        throttleb = args.throttleb.split('-')
        if len(throttleb) == 2:
            min = int(throttleb[0])
            max = int(throttleb[1])
            if min > 0 and max > 0 and min < max:
                tb = [min, max]            

    banned = 900
    if args.banned:
        banned = args.banned
            
    verbose = False
    if args.verbose:
        verbose = True

    debug = False
    if args.debug:
        debug = True

    if verbose:
        print ("Parameter :", search, loc, output, ta, tb, banned, verbose, debug)

    return  search, loc, output, pages, ta, tb, banned, verbose, debug


def ready_categories_cities(search, loc):
    # open input files
    try:
        search_file = open(search, 'r', encoding='utf-8')
    except IOError:
        print ("Could not read categories file :", search)
        sys.exit(-1)

    try:
        loc_file = open(loc, 'r', encoding='utf-8')
    except IOError:
        print ("Could not read city file :", loc)
        sys.exit(-1)

    # ready categories
    categories = []
    with search_file:
        category_lines = [line.strip() for line in search_file.readlines()]
        # remove duplicate items
        categories = list(set(category_lines))
        if len(categories) == 0:
            print ("Empty categories. Check the categories input file.")
            raise InvalidFileError
        categories = sorted(categories)
    
    # ready cities
    cities = []
    with loc_file:
        cities = [line.strip() for line in loc_file.readlines()]
        if len(cities) == 0:
            print ("Empty cities. Check the city input file.")
            raise InvalidFileError
        
    return categories, cities


def make_yelp_search_url(category, city, page, links_per_page):
    category = quote(category)
    city = quote(city)
    if page == 0:
        search_url = "https://www.yelp.com/search?find_desc={}&find_loc={}".format(category, city)
    else:
        start = page * links_per_page
        search_url = "https://www.yelp.com/search?find_desc={}&find_loc={}&start={}".format(category, city, start)
        
    return search_url


def check_banned(soup):
    head = soup.head
    if head == None:
        return True

    meta_tags = head.select("meta")
    if len(meta_tags) < 2:
        return True
    
    scripts = soup.select("script", {"type": "text/javascript"})
    if scripts == None or len(scripts) < 2:
        return True
    if scripts[0]['src'] == '/error-pages/block.js':
        return True
    
    return False


def get_business_links(soup):
    links = []
    li_items = soup.select("li")
    for item in li_items:
        link = item.find("a")
        if link:
            link = link['href']
            if link.find("/biz/") == 0:
                link = unquote(link)
                link = "https://www.yelp.com" + link
                links.append(link)
    return links


def check_has_nextpage(soup):
    next_page_item = soup.find("a", {"class": "next-link"})
    if next_page_item:
        return True
    else:
        return False
    

def crawl_yelp(driver, category, city, pages, ta, output_file, verbose):
    if verbose:
        msg = "--- Search category: {}, city: {}".format(category, city)
        print (msg)

    try:

        biz_links = []
        links_per_page = 10
        links_fetched = False
        for page in range(pages):    
            search_url = make_yelp_search_url(category, city, page, links_per_page)

            if verbose:
                msg = "------ Crawling page: {}".format(page+1)
                print (msg)
                        
            driver.get(search_url)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            
            # Check if the Yelp allows browsing
            if check_banned(soup) == True:
                return False
            
            links = get_business_links(soup)
            if len(links) == 0:
                break
            for link in links:
                biz_links.append(link)
            if verbose:
                msg = "------ Crawled {} business links".format(len(biz_links))
                print (msg)

            # Check if the next page is exist
            if check_has_nextpage(soup) == False:
                break

            # Determine links count per page
            if links_fetched == False:
                links_per_page = len(biz_links)
                if links_per_page > 20:
                    links_per_page = 30
                else:
                    links_per_page = 10
                links_fetched = True
                
            # throttling between pages
            wait_for(ta[0], ta[1])

        # remove duplicate items
        biz_links = list(set(biz_links))
        for link in biz_links:
            output_file.write(link)
            output_file.write('\n')

        output_file.flush()
        
        return True
    except TimeoutException:
        print ("Network Error occurred.")
        return False
    except ProgramKilled:
        print ("Exit script...")
        driver.quit()
        sys.exit(0)
    except:
        print ("Unknown Error occurred.")
        return False
    

if __name__ == "__main__":

    # signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Parse arguments
        search, loc, output, pages, ta, tb, banned, verbose, debug = parse_argument()
        
        # Read categories and cities from the input files.
        categories, cities = ready_categories_cities(search, loc)
        if verbose:
            print ("Category count  =", len(categories))
            print ("City count      =", len(cities))

        driver = get_driver(debug)
        with open(output, "w", encoding='utf-8') as output_file:
            if verbose:
                print ("Main Processing...")

            for city in cities:
                for category in categories:
                    while True:
                        crawled = crawl_yelp(driver, category, city, pages, ta, output_file, verbose)
                        if crawled == True:
                            wait_for(tb[0], tb[1])
                            break
                        else:
                            msg = "The Yelp is banned, wait for {} seconds...".format(banned)
                            print (msg)
                            driver.quit()
                            driver = get_driver()
                            wait_for(banned, banned+10)
    except:
        sys.exit(0)
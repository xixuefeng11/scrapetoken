from multiprocessing.pool import ThreadPool, Pool
import requests
import sys
import threading
import argparse
import time, signal
from bs4 import BeautifulSoup
import random
import json
from urllib.parse import unquote


RATING_THRESHOLD = 3.5

headers = {'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding':'gzip, deflate, br',
    'Accept-Language':'en-GB,en;q=0.9,en-US;q=0.8,ml;q=0.7',
    'Cache-Control':'max-age=0',
    'Connection':'keep-alive',
    'Host':'www.yelp.com',
    'Upgrade-Insecure-Requests':'1',
    'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36'
}

    
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
    

def get_business_website_url(params):
    
    page_urls   = params['urls']
    throttle    = params['throttle']
    output_file = params['output']
    verbose     = params['verbose']
    
    for page_url in page_urls:

        response = requests.get(page_url, headers=headers)
        if response.status_code==200:
            soup = BeautifulSoup(response.text, 'lxml')

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
                print ("Rating Value :", ratingValue, page_url)
                
            if ratingValue > RATING_THRESHOLD:
                wait_for(throttle - 0.5, throttle + 0.5)
                continue
            
            # biz_website_span = soup.find("span", {"class", "biz-website"})
            # if biz_website_span == None:
            #     if verbose == True:
            #         print ("There is no business website")
            #     wait_for(throttle - 0.5, throttle + 0.5)
            #     continue
            
            website_url = ""
            biz_website_link = soup.find('a', {'rel': 'noopener'})
            # biz_website_link = biz_website_span.find("a", {"rel": "noopener nofollow"})
            if biz_website_link:
                website_url = biz_website_link['href']
                website_url = get_substring(website_url, "url=", "&website")
                if website_url != "":
                    website_url = unquote(website_url)
                    print ("Business website :", website_url)
                
                    output_file.write(website_url)
                    output_file.write("\n")
                    
            if website_url == "" and verbose == True:
                print ("There is no business website.")

            wait_for(throttle - 0.5, throttle + 0.5)
    

def thread_proc(params):
    get_business_website_url(params)
    
    
def divide_chunks(urls, n):
    total_count = len(urls)
    chunk_size = int(total_count / n) + 1
    
    for i in range(0, total_count, chunk_size):  
        yield urls[i:i + chunk_size]
        

if __name__ == "__main__":

    """
    Command : python yelp_proc.py -i input.txt -o output.txt -t 3 -s 1 -v
    """

    parser = argparse.ArgumentParser(description='Extracts business website urls which have rating value \
        less than 3.5 from the input plain text file. Format: \n\tpython yelp_proc.py -i <input file name> -o <output file name> -t <thread count> -s <throttle seconds> -v')
    parser.add_argument('-i', '--input', type=str, help='input file name(in plain text format)')
    parser.add_argument('-o', '--output', type=str, help='output file name(wesite url in each row)')
    parser.add_argument('-t', '--threads', type=int, help='thread count. must be less than 5')
    parser.add_argument('-s', '--secondrate', type=int, help='throttle time between requests in seconds')
    parser.add_argument('-v', '--verbose', action='store_true', help='verbose mode')
    

    args = parser.parse_args()
    
    if (args.input is None or args.output is None):
        print ("input/output parameter must be given!")
        parser.print_help()
        sys.exit(0)
    
    threads = 1
    if args.threads:
        threads = int(args.threads)
        if threads > 16:
            threads = 16
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
        
    if verbose:
        print ("Parameter", args.input, args.output, threads, throttle, verbose)
        
        
    # signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)   
    
    with open(args.input, "r", encoding='utf-8') as fInput:
        with open(args.output, "w", encoding='utf-8') as fOutput:
            
            # read urls from the input file
            url_list = [line.strip() for line in fInput.readlines()]
            if verbose:
                print ("Url count =", len(url_list))

            # make parameters for the thread
            params = []
            url_chunks = list(divide_chunks(url_list, threads))
            for i in range(threads):
                param = {
                    "urls": url_chunks[i],
                    "throttle": throttle,
                    "output": fOutput,
                    "verbose": verbose
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

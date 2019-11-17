# Description
Scrape Yelp for the redirect URL of up to a specified number of pages for a search footprint which is provided as an input file.

# Environment
python 3.6.8+

# Running script
pip install -r requirements.txt

python script.py -s <categories_file> -l <city_file> -o <output_file> -p <page_count> -ta <throttling_value_range_1> -tb <throttling_value_range_2> -b <waiting_time_when_banned> -v

## Samples:
    python script.py -s categories.txt -l city.txt -o output.txt -p 10 -ta 5-10 -tb 2-5 -b 900 -v
    python script.py -s categories.txt -l city.txt -o output.txt

## arguments
1. -s(--search)     : categories file name(in plain text format), required
2. -l(--loc)        : city file name(in plain text format), required
3. -o(--output)     : output file name(wesite url in each row), required
4. -p(--pages)      : page count. default: 10
5. -ta(--throttlea) : throttle time range between pages in seconds. default: 5-10
6. -tb(--throttleb) : throttle time range between searches in seconds. default: 2-5
7. -b(--banned)     : waiting time in seconds when banned. default: 900
8. -v(--verbose)    : verbose mode


## chrome driver version
chrome driver version in this project is 74.0.3729.
Download chrome driver[https://chromedriver.chromium.org/downloads] for your google chrome brower version and copy it in driver directory.

# Description
Extracts business website urls which have rating value less than 3.5 from the input plain text file.

# Environment
python 3.6.8+

# Running script
pip install -r requirements.txt

## requests mode
python yelp_proc_req.py -i <input file name> -o <output file name> -t <thread count> -s <throttle seconds> -v

## webdriver mode
python yelp_proc.py -i <input file name> -o <output file name> -t <thread count> -s <throttle seconds> -v

## arguments
1. -i : input file name (yelp business url in each row)
2. -o : output file name
3. -t : thread count
4. -s : throttling time in seconds
5. -v : verbose mode

## Note
There are 2 modes, that is, requests(python requests) mode and webdriver(python selenium webdriver) mode.
Yelp.com uses distil service to protect bot scraping.
Recommend to use webdriver mode, otherwise the Yelp.com can block your ip address.

## chrome driver version
chrome driver version in this directory is 74.0.3729.
Download chrome driver for your google chrome brower version.
https://chromedriver.chromium.org/downloads
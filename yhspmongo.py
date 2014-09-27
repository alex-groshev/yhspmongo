import sys
import argparse
import requests
from datetime import datetime
from pymongo import MongoClient


def scrape(symbol):
    url = 'http://ichart.yahoo.com/table.csv?s=%s&ignore=.csv' % symbol
    r = requests.get(url, timeout=10)
    if r.status_code == requests.codes.ok:
        return r.text
    else:
        print r.status_code
        exit()


def yahoo_historical_prices(symbol):
    daily_data = scrape(symbol).strip().split("\n")
    records = []
    for day in daily_data[1:]:
        vals = day.split(',')
        d = {
            'Symbol': symbol,
            'Date': datetime.strptime(vals[0], '%Y-%m-%d'),
            'Open': vals[1],
            'High': vals[2],
            'Low': vals[3],
            'Close': vals[4],
            'Volume': int(vals[5]),
            'AdjClose': vals[6]
        }
        records.append(d)
    return records


def save(host, port, prices):
    client = MongoClient(host, port)
    db = client.yhsp
    for p in prices:
        try:
            db.daily_prices.insert(p)
        except:
            print 'Unexpected error:', sys.exc_info()[0], sys.exc_info()[1]


def main():
    ap = argparse.ArgumentParser('yhspmongo saves historical stock prices from Y! finance to MongoDB.')
    ap.add_argument('--s', dest='symbol', required=True, help='Stock')
    ap.add_argument('--h', dest='host', required=True, help='MongoDB host')
    ap.add_argument('--p', dest='port', required=True, help='MongoDB port')
    args = ap.parse_args()

    prices = yahoo_historical_prices(args.symbol)
    save(args.host, int(args.port), prices)
    print 'DONE'


if __name__ == '__main__':
    main()
# OLD STUFF NOT CURRENTLY USED

import urllib2

def main():


    ticker = 'AAPL.O'
    query = 'http://hopey.netfonds.no/tradedump.php?date=20150807&paper=FB.O&csv_format=txt'
    f = urllib2.urlopen(query)
    s = f.read()
    f.close()
    print s


main()
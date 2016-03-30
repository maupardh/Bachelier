# OLD STUFF NOT CURRENTLY USED

import urllib3

def main():


    ticker = 'AAPL.O'
    query = 'http://hopey.netfonds.no/tradedump.php?date=20150807&paper=FB.O&csv_format=txt'
    f = urllib3.urlopen(query)
    s = f.read()
    f.close()
    print (s)


main()
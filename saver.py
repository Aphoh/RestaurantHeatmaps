import sys

import unicodecsv as csv
from rx import Observer

from yelp import yelpworker


# Observable which writes all restaurants out to a csv
class CsvObserver(Observer):
    def __init__(self, csv_writer, f):
        self.csv_writer = csv_writer
        self.f = f
        csv_writer.writeheader()

    def on_next(self, rest):
        row = rest._asdict()
        self.csv_writer.writerow(row)

    def on_completed(self):
        self.f.close()

    def on_error(self, row):
        self.f.close()
        print "Error {0}".format(row)


lls = [[37.8684932,-122.2785197], [37.951277, -122.339450], [37.963733, -122.583896], [37.706689, -122.446567], [37.756112, -122.491886], [37.534835, -122.306492], [37.440854, -122.191136], [37.440854, -122.191136], [37.771628, -122.193143], [37.748154, -122.166364], [37.713131, -122.150571], [37.647380, -122.081906]]

#with open(sys.argv[1], 'r') as fin:
#    reader = csv.reader(fin)
#    i = iter(reader)
#    next(i)  # Remove header
#    for row in reader:
#        lls.append([float(row[0]), float(row[1])])

fout = open(sys.argv[1], 'wb')

# Start querying yelp for restaurants
writer = csv.DictWriter(fout, yelpworker.fields)
observer = CsvObserver(writer, fout)
yelpworker.restaurants(lls).subscribe(observer)
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


lls = [[37.971679, -122.326883],
    [37.961188, -122.534115],
    [37.953495, -122.631730],
    [38.011414, -122.290119],
    [38.079603, -122.236970],
    [38.000884, -122.282193],
    [37.932708, -122.511528],
    [37.888540, -122.457902],
    [37.892879, -122.552934],
    [38.059817, -122.555898],
    [37.868592, -122.289084],
    [37.872939, -122.264820],
    [37.888491, -122.274648],
    [37.888491, -122.274648],
    [37.910874, -122.296651],
    [37.927738, -122.315319],
    [37.931263, -122.348750],
    [37.841801, -122.275243],
    [37.822417, -122.281766],
    [37.830828, -122.253614],
    [37.765860, -122.254773],
    [37.753107, -122.193104],
    [37.710979, -122.145841],
    [37.624635, -122.062886],
    [37.772181, -122.421614],
    [37.779738, -122.392442],
    [37.761114, -122.438665],
    [37.713964, -122.472691],
    [37.723759, -122.427708],
    [37.807698, -122.417265],
    [37.801001, -122.416718],
    [37.689608, -122.472915],
    [37.905914, -122.541456],
    [37.928260, -122.512597],
    [37.654196, -122.084043],
    [37.414143, -122.151047],
    [37.360049, -122.029014],
    [37.423362, -122.134118],
    [37.474025, -122.150207],
    [37.535985, -121.989588],
    [37.426323, -121.909928],
    [37.533550, -121.988523],
    [37.471858, -122.171668],
    [37.348958, -122.020343],
    [37.385132, -121.952330]]


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
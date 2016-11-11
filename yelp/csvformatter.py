import csv
import json
import math
import os
import sys
from collections import Counter

#
#   Each row -> object identifier (type_rating_cost)
#   Count each object identifier
#

ri = 6
pi = 7
c1 = 9
c2 = 10
c3 = 11

name_to_alias_mapping = {}
with open('../category_mapping.json', 'r') as fin:
    catjson = json.loads(fin.read())
    for cat in catjson:
        name_to_alias_mapping[cat["title"]] = cat["alias"]

correction_alias_mapping = {}
final_cats = []
with open('../category_correction.csv', 'r') as fin:
    r = csv.reader(fin)
    for row in r:
        if row and row[0]:
            correction = row[0]
            final_cats.append(correction)
            for key in row[1:]:
                if key:
                    correction_alias_mapping[key] = correction

numbs = ["zero", "one", "two", "three", "four", "five"]

all_keys_dict = {}
for cat in final_cats:
    for rating in numbs[3:6]:
        for price in numbs[1:3]:
            all_keys_dict["{0}_{1}_{2}".format(cat, rating, price)] = ""

for rating in numbs[3:6]:
    for price in numbs[1:3]:
        all_keys_dict["total_{0}_{1}".format(rating, price)] = ""

all_keys_dict['total'] = ''
all_keys_dict['filename'] = ''


def to_identifier(row):
    r = numbs[int(math.ceil(float(row[ri])))]
    if int(row[pi]) > 2:
        print "bad price: {0}".format(row[pi])
        return []
    p = numbs[int(row[pi])]

    ids = []

    for c in [c1, c2, c3]:
        premap = name_to_alias_mapping[row[c1]]
        if premap in correction_alias_mapping:
            print "correct mapping"
            c = correction_alias_mapping[premap]
            ids.append("{0}_{1}_{2}".format(c, r, p))
        else:
            print "incorrect mapping"

    return ids


def get_totals_for_rating_price_pairs(dict):
    subtotals = {}
    for k, v in dict.iteritems():
        s = k.split("_")
        skey = "total_{0}_{1}".format(s[-2], s[-1])
        if skey in subtotals:
            subtotals[skey] += v
        else:
            subtotals[skey] = v
    return subtotals

for _, _, files in os.walk(sys.argv[1]):
    dicts = []
    for file in files:
        if "." in file and "csv" in file:
            with open(file, 'r') as fin:
                d = dict(all_keys_dict)
                r = iter(csv.reader(fin))
                next(r)
                identifierslists = map(lambda x: to_identifier(x), r)
                identifiers = reduce(lambda x, y: x + y, identifierslists)
                c = Counter(identifiers)
                totals = get_totals_for_rating_price_pairs(c)
                if len(sys.argv) > 3:
                    c = {k: float(v) / float(len(identifiers)) for k, v in c.iteritems()}
                d.update(c)
                d.update({'total': len(identifiers), 'filename': file})
                d.update(totals)
                dicts.append(d)
                print d

    with open(sys.argv[2], 'wb') as fin:
        dr = csv.DictWriter(fin, all_keys_dict.keys())
        dr.writeheader()
        print dicts[0]
        dr.writerows(dicts)

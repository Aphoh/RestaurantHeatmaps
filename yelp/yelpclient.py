import urllib
from ConfigParser import ConfigParser
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import time
from shapely.geometry import Polygon, Point
import json
from collections import namedtuple
from unidecode import unidecode

cfg = ConfigParser()
cfg.readfp(open('config/yelp.cfg'))
client_id = cfg.get('api', 'app_id')
client_secret = cfg.get('api', 'app_secret')

client = BackendApplicationClient(client_id=client_id)
oauth = OAuth2Session(client=client)
token = oauth.fetch_token(token_url="https://api.yelp.com/oauth2/token", client_id=client_id,
                          client_secret=client_secret)

base_url = "https://api.yelp.com/v3/businesses/search?"

fields = ['id', 'lat', 'lon', 'name', 'city', 'address', 'rating', 'price', 'url', 'category1', 'category2',
          'category3']
Restaurant = namedtuple('Restaurant', fields)


#
#   Format's a yelp location dictionary as a readable address
#
def __format_address(location_dict):
    # x ave, city, state zip

    return "{0}, {1}, {2} {3}".format(
        unidecode(location_dict["address1"]), unidecode(location_dict["city"]), unidecode(location_dict["state"]), unidecode(location_dict["zip_code"]))


#
#   Returns whether a given yelp business dictionary is a valid restaurant
#
def __is_valid(business_map):
    basic = {'name', 'location', 'rating', 'categories', 'price'}
    in_loc = {'city', 'address1', 'state', 'zip_code'}
    valid = basic <= set(business_map) and in_loc <= set(business_map["location"])

    for i in in_loc:
        if not business_map["location"][i]:
            return False

    if not valid:
        print "Invalid business?: {0}".format(json.dumps(business_map))

    return valid


#
#   Converts a business dictionary to a Restaurant namedtuple
#
def __restaurant_object(business_map):
    cats = business_map["categories"]
    c2 = cats[1]["title"] if len(cats) > 1 else "None"
    c3 = cats[2]["title"] if len(cats) > 2 else "None"
    return Restaurant(
        id=business_map["id"],
        lat=business_map["coordinates"]["latitude"],
        lon=business_map["coordinates"]["longitude"],
        name=business_map["name"],
        city=business_map["location"]["city"],
        address=__format_address(business_map["location"]),
        rating=business_map["rating"],
        price=len(business_map["price"]),
        url=business_map["url"],
        category1=cats[0]["title"],
        category2=c2,
        category3=c3
    )


#
#   Gets the query url for a given lat and long
#
def __query_url(lat, lng, limit=50, offset=0, radius=40000):
    url = base_url + urllib.urlencode(
        {'latitude': lat, 'longitude': lng, 'limit': limit, 'offset': offset, 'radius': radius}
    )
    return url


#
#   Gets all restaurants in a given lat and long
#
def get_restaurants(lat, lng, limit=50):
    url = __query_url(lat, lng, limit)
    results = oauth.get(url)
    if results.status_code is not 200:
        print "Error {0} processing request, sleeping for 10 seconds and retrying..."
        time.sleep(10)
        return get_restaurants(lat, lng, limit=limit)

    return results.json()["businesses"]


def page_restaurants(lat, lng, limit=50, offset=0, threshold=1000):
    print "Fetching data"
    url = __query_url(lat, lng, limit=limit, offset=offset)
    req = oauth.get(url)
    if req.status_code is not 200:
        print "Error {0} processing request, sleeping for 5 seconds and retrying...\n {1}".format(req.status_code,
                                                                                                  req.reason)
        time.sleep(7)
        return page_restaurants(lat, lng, limit=limit, offset=offset)
    if offset + 50 >= threshold:
        return req.json()["businesses"]
    else:
        return req.json()["businesses"] + page_restaurants(lat, lng, limit=50, offset=offset + 50)


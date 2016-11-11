import json
from collections import namedtuple

from rx import Observable

import yelpclient as y

fields = ['id', 'lat', 'lon', 'name', 'city', 'address', 'rating', 'price', 'url', 'category1', 'category2', 'category3']
Restaurant = namedtuple('Restaurant', fields)


#
#   Observable which fetches all restaurant data (in yelp's json format)
#
def __fetch_all_restaurant_data(lat_long_list):
    return Observable.from_(lat_long_list).flat_map(lambda x: y.page_restaurants(x[0], x[1]))


#
#   Format's a yelp location dictionary as a readable address
#
def __format_address(location_dict):
    # x ave, city, state zip
    return "{0}, {1}, {2} {3}".format(
        location_dict["address1"], location_dict["city"], location_dict["state"], location_dict["zip_code"])


#
#   Returns whether a given yelp business dictionary is a valid restaurant
#
def __is_valid(business_map):
    basic = {'name', 'location', 'rating', 'categories', 'price'}
    in_loc = {'city', 'address1', 'state', 'zip_code'}
    valid = basic <= set(business_map) and in_loc <= set(business_map["location"])

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
#   Transforms all the requests into unique, valid Restaurant namedtuples
#
def __transform_request(base):
    filtered = base.filter(lambda x: __is_valid(x))
    formatted = filtered.map(lambda x: __restaurant_object(x))
    grouped = formatted.group_by(lambda x: x.id)  # Obs<Restaurant> -> Obs<GroupObs<Restaurant>>
    unique = grouped.flat_map(lambda x: x.take(1))  # Obs<GroupObs<Restaurant>> -> Obs<Restaurant>
    return unique


#
#   Returns an observable which fetches and formats all restaurant data from yelp as a Restaurant namedtuple
#
def restaurants(lat_long_list):
    base = __fetch_all_restaurant_data(lat_long_list)
    return __transform_request(base)


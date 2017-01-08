import pandas as pd
import requests
import simplejson as json
import collections
import math
import datetime
import numpy as np
import re
import warnings
import collections
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob
from langdetect import detect
warnings.filterwarnings('ignore')


# THIS IS THE MAIN AIRBNB DIRECTORY SCRAPING PIPELINE. THIS PART RETURNS AND
# PARSES THE RESULTS. THIS IS WHAT THE LATER PARTS BUILD OFF OF.

def zip_url(postcode, page = 1):
    base_url = "https://www.airbnb.co.uk/search/search_results?page="
    base_location = "&location="
    post_url = base_url + str(page) + base_location + postcode
    return post_url

def extract_data_url(this_url):
    url_response = requests.get(this_url)
    return url_response.json()

def confirm_location(data):
    location = data["results_json"]["metadata"]["breadcrumbs"][2]["location_name"]
    return location == 'London'

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def summary_stats(data):
    summary_data = {}
    meta = data["results_json"]["metadata"]
    get_info = ['avg_price_by_room_type', 'guided_search']
    for info in get_info:
        summary_data.update(meta[info])
    summary = flatten(summary_data)
    summary['price_entire'] = summary.pop('avg_price_Entire home/apt')
    summary['price_private'] = summary.pop('avg_price_Private room')
    summary['min_review'] = summary.pop('min_review_count')
    summary['min_stars'] = summary.pop('min_stars')
    summary['ratio_entire'] = summary.pop('ratio_Entire home/apt')
    summary['ration_private'] = summary.pop('ratio_Private room')
    summary.pop('avg_gbm_score', None)
    summary.pop('avg_usd_price', None)
    summary.pop('ratio_Shared room', None)
    return summary

def num_results_iter(data):
    results = data["results_json"]["metadata"]["facets"]["bedrooms"][-1]["count"]
    iterations = data["results_json"]["metadata"]["pagination"]["result_count"]
    iterations = math.ceil(results/iterations)
    return [results, iterations]

def detail_listing(data):
    results = data['results_json']['search_results']
    for index in range(len(results)):
        results[index] = flatten(results[index])
        results[index].pop('listing_airbnb_plus_enabled', None)
        results[index].pop('listing_coworker_hosted', None)
        results[index].pop('listing_instant_bookable', None)
        results[index].pop('listing_is_business_travel_ready', None)
        results[index].pop('listing_is_new_listing', None)
        results[index].pop('listing_listing_tags', None)
        results[index].pop('listing_picture_url', None)
        results[index].pop('listing_picture_urls', None)
        results[index].pop('listing_primary_host_thumbnail_url', None)
        results[index].pop('pricing_quote_is_good_price', None)
        results[index].pop('pricing_quote_check_out', None)
        results[index].pop('pricing_quote_check_in', None)
        results[index].pop('listing_user_thumbnail_url', None)
        results[index].pop('listing_primary_host_thumbnail_url', None)
        results[index].pop('viewed_at', None)
        results[index].pop('listing_extra_host_languages', None)
        results[index].pop('listing_localized_city', None)
        results[index].pop('pricing_quote_guests', None)
        results[index]['bedrooms'] = results[index].pop('listing_bedrooms')
        results[index]['beds'] = results[index].pop('listing_beds')
        results[index]['room_id'] = results[index].pop('listing_id')
        results[index]['lat'] = results[index].pop('listing_lat')
        results[index]['long'] = results[index].pop('listing_lng')
        results[index].pop('listing_name', None)
        results[index]['capacity'] = results[index].pop('listing_person_capacity')
        results[index].pop('listing_picture_count', None)
        results[index].pop('listing_primary_host_first_name', None)
        results[index]['host_id'] = results[index].pop('listing_primary_host_id')
        results[index]['superhost'] = results[index].pop('listing_primary_host_is_superhost')
        results[index]['home_type'] = results[index].pop('listing_room_type')
        results[index].pop('listing_public_address', None)
        results[index]['num_reviews'] = results[index].pop('listing_reviews_count')
        results[index]['stars'] = results[index].pop('listing_star_rating')
        results[index].pop('listing_user_first_name', None)
        results[index].pop('listing_user_id', None)
        results[index].pop('listing_user_is_superhost', None)
        results[index].pop('pricing_quote_available', None)
        results[index].pop('pricing_quote_average_booked_price', None)
        results[index]['instant_book'] = results[index].pop('pricing_quote_can_instant_book')
        results[index]['price'] = results[index].pop('pricing_quote_rate_amount')
        results[index].pop('pricing_quote_rate_currency', None)
        results[index].pop('pricing_quote_rate_type', None)
    return results

def airbnb_by_postcode(take_postcode):
    first_page = extract_data_url(zip_url(take_postcode))

    if not(confirm_location(first_page)):
        return ["ERROR! THIS IS NOT LONDON DATA", "ERROR! THIS IS NOT LONDON DATA", "ERROR! THIS IS NOT LONDON DATA"
               , "ERROR! THIS IS NOT LONDON DATA"]

    all_data = []
    all_data = all_data + detail_listing(first_page)

    for page_num in range(2, (num_results_iter(first_page)[1] + 1)):
        other_page = extract_data_url(zip_url(take_postcode, page_num))
        all_data = all_data + detail_listing(other_page)

    return [summary_stats(first_page), all_data]



# IMPORTANT DOCUMENTATION: THIS TAKES IN A POSTCODE WITHOUT SPACES OR ANY FUNKY SHIT.
# IT RETURNS A LIST: SUMMARY STATS BY POSTCODE, ALL DATA BY POSTCODE,
# HOSTIDS BY POSTCODE, ROOMSIDS BY POSTCODE

def overview(post_code):
    viz_list = airbnb_by_postcode(post_code)
    summ = viz_list[0]
    all_rooms = pd.DataFrame.from_dict(viz_list[1])
    hosts = list(all_rooms["host_id"])
    rooms = list(all_rooms["room_id"])
    return [summ, all_rooms, hosts, rooms]


# THIS IS THE UTILIZAtiON STATS BIT. PULLING UTILIZATION FOR ANY PROPERTY BY ID

def pull_util_data(room_id):
    base_url = "https://www.airbnb.com/api/v2/calendar_months?key=d306zoyjsyarp7ifhu67rjxn52tv0t20&listing_id="
    id_onwards = "&month="
    month_onwards = "&year=2017&count=3&_format=with_conditions"
    stitched = base_url + str(room_id) + id_onwards + str(datetime.datetime.now().month) + month_onwards
    url_response = requests.get(stitched)
    json_data = url_response.json()
    json_data = json_data['calendar_months']
    return json_data

def get_raw_dates(data):
    true_dates = []
    false_dates = []
    ignore_before = datetime.datetime.now()
    for index in range(len(data)):
        new_data = data[index]["days"]
        for this_data in range(len(new_data)):
            if new_data[this_data]['available']:
                true_dates = true_dates + [new_data[this_data]["date"] + "|" + str(new_data[this_data]["price"]["local_price"])]
            else:
                false_dates = false_dates + [new_data[this_data]["date"] + "|" + str(new_data[this_data]["price"]["local_price"])]
    true_dates = list(set(true_dates))
    false_dates = list(set(false_dates))
    true_dates.sort()
    false_dates.sort()
    max1 = true_dates[-1]
    max2 = false_dates[-1]
    max_of_max = max(max1, max2).split("|")[0].split("-")
    max_date = datetime.datetime(int(max_of_max[0]), int(max_of_max[1]), int(max_of_max[2]))
    return list(true_dates), list(false_dates), max_date

def util_stats(true_dates, false_dates, maxdate):
    real_false_dates = []
    false_data = []
    right_now = datetime.datetime.now()
    for this_date in false_dates:
        temp_date = this_date.split("|")[0].split("-")
        ye = int(temp_date[0])
        mo = int(temp_date[1])
        da = int(temp_date[2])
        comp_date = datetime.datetime(ye, mo, da)
        if comp_date > right_now:
            real_false_dates = real_false_dates + [this_date]

    div = ((maxdate - right_now)/3)
    util1 = right_now + div
    util2 = util1 + div
    day_30_false = []
    day_60_false = []
    day_90_false = []
    day_30_true = []
    day_60_true = []
    day_90_true = []
    rev30 = 0
    rev60 = 0
    rev90 = 0

    for this_date in real_false_dates:
        dat = this_date.split("|")[0].split("-")
        ye = int(dat[0])
        mo = int(dat[1])
        da = int(dat[2])
        dat = datetime.datetime(ye, mo, da)
        price = int(this_date.split("|")[1])

        if dat < util1:
            day_30_false = day_30_false + [str(dat)]
            rev30 = rev30 + price
        elif dat < util2:
            day_60_false = day_60_false + [str(dat)]
            rev60 = rev60 + price
        elif dat <= maxdate:
            day_90_false = day_90_false + [str(dat)]
            rev90 = rev90 + price

    for this_date in true_dates:
        dat = this_date.split("|")[0].split("-")
        ye = int(dat[0])
        mo = int(dat[1])
        da = int(dat[2])
        dat = datetime.datetime(ye, mo, da)

        if dat < util1:
            day_30_true = day_30_true + [str(dat)]
        elif dat < util2:
            day_60_true = day_60_true + [str(dat)]
        elif dat <= maxdate:
            day_90_true = day_90_true + [str(dat)]

    perc30 = len(day_30_false)/(len(day_30_true) + len(day_30_false))
    perc60 = len(day_60_false)/(len(day_60_true) + len(day_60_false))
    perc90 = len(day_90_false)/(len(day_90_true) + len(day_90_false))

    return [[rev30, perc30, day_30_false], [rev60, perc60, day_60_false], [rev90, perc90, day_90_false]]

## IMPORTANT POSTCODE: THIS TAKES IN A LIST OF ROOMIDS AND RETURNS A UTILIZATION STATISTC BY ROOM.
## THE UTIL STATS ARE NESTED LISTS WITH ONE NEST PER ENTRY. SUBINDEX FOR EACH LIST.
## SUBSUB INDEX IS THE ROOMID AND NEXT INDEX IS THE LIST OF UTIL STATS.
## UTIL STATS ARE 30, 60 and 90 DAY RESPECTIVELY. SUBSUBSUB INDEX IS MONEY, PCTAGE AND DATES.

def util(room_ids):
    util_by_room = []
    for room_id in room_ids:
        room_data = pull_data(room_id)
        t_dates, f_dates, m_date = get_raw_dates(room_data)
        util_room = util_stats(t_dates, f_dates, m_date)
        util_by_room = util_by_room + [[room_id, util_room]]
    return util_by_room

# THIS IS THE PIPELINE FOR PULLING THE LISTING DATA BY THE LIST OF ROOM IDS. THIS IS GOING TO
# BE SOME NICE AND DETAILED LISTING DATA - MAINLY THE STARS BREAKDOWN.

def vasilis_scrape_property(room_id):
    source_code = requests.get("https://www.airbnb.co.uk/rooms/"+str(room_id)).text
    master_dict = {}

    #get the host first name
    regex = re.search('host_first_name":".*?"', source_code)
    if regex is None:
        return "This listing is no longer available"
    regex = regex.group(0)
    host_name = regex.split('"')[-2]
    master_dict['host_name'] = host_name

    #room type
    regex = re.search('"room_type_category":".*?"', source_code).group(0)
    room_type = regex.split('"')[-2]
    master_dict['room_type'] = room_type

    #cancellation policy
    regex = re.search('"cancellation_policy_label":".*?"', source_code).group(0)
    cancel_policy = regex.split('"')[-2]
    master_dict['cancellation_policy'] = cancel_policy

    #minimum stay
    regex = re.search('"localized_minimum_nights_description":".*?"', source_code).group(0)
    if len(re.findall(r'\d+',regex))>0:
        min_stay = re.findall(r'\d+',regex)[0]
    else:
        min_stay=0
    master_dict['min_stay_nights'] = int(min_stay)

    # The space section
    full_results = re.findall('\\[\\{.*?\\}\\]', source_code)
    lst = ['"Accommodates:"','"Bathrooms:"','"Bedrooms:"','"Beds:"','"Property type:"']
    matches = []
    for item in full_results:
        if any(tag in item for tag in lst):
            matches.append(item)
    matches = list(set(matches))
    all_jsons = json.loads(matches[0])
    for item in all_jsons:
        if '"'+item['label']+'"' in lst:
            master_dict[item['label']] = item['value']


    # Desciption
    regex = re.search('"description":".*?"', source_code).group(0)
    desc = regex.split('"')[-2]
    master_dict['description'] = desc

    # Star Rating Breakdown
    rating_tags = ['Accuracy', '"Communication"', '"Cleanliness"', '"Location"', '"Check In"','"Value"']
    full_results = re.findall('\\[\\{".*?\\}\\]', source_code)
    matches = []
    for item in full_results:
        if any(tag in item for tag in rating_tags):
            matches.append(item)
    matches = list(set(matches))
    star_breakdown = {}
    if len(matches)>0:
        # print(matches[0])
        matches = json.loads(matches[0])
        for item in matches:
            star_breakdown[item['label']] = item['value']
        master_dict['rating_breakdown'] = star_breakdown
    else:
        master_dict['rating_breakdown'] = {'Accuracy':None, 'Communication':None, 'Cleanliness':None,
                                          'Location':None, 'Check In':None, 'Value':None}

    # Amenities
    amenities = ["Wireless Internet", "Air Conditioning", "Kitchen", "Cable TV", "Kitchen", "TV", "24-hour check-in",
                "Pool", "Washer", "Iron", "Free parking on premise", "Laptop friendly workspace", "Heating",
                "Indoor Fireplace", "Elevator in Building", "Essentials", "Dryer", "Hangers", "Internet", " Shampoo",
                "Family/Kid Friendly", "Gym", "Pets Allowed", "Wheelchair accessible", "Smoking Allowed",
                "Buzzer/wireless intercom", "Suitable for events", "Doorman", " Hair dryer", "Private living room",
                "Breakfast", "Private entrance", "Hot Tub"]
    lst = re.findall(r'{"explore_url":.*?}', source_code)
    matches = []
    for item in lst:
        if any(tag in item for tag in amenities):
            matches.append(json.loads(item))
    matches
    ams = {}
    for d in matches:
        ams[d['name']] = d['is_present']
    master_dict['Amenities'] = ams

    # Discounts
    weekly = re.findall(r'"weekly_discount":{.*?}', source_code)
    if len(weekly)!=0:
        d = json.loads(weekly[0][18:])
        master_dict['weekly_discount'] = d['value']
    else:
        master_dict['weekly_discount'] = None

    monthly = re.findall(r'"monthly_discount":{.*?}', source_code)
    if len(monthly)!=0:
        d = json.loads(monthly[0][19:])
        master_dict['monthly_discount'] = d['value']
    else:
        master_dict['monthly_discount'] = None

    return master_dict

## THIS STRAIGHT UP SPITS OUT A DATAFRAME WITH ALL THE RELEVANT PROPERTY FIELDS LOADED INTO IT
## FOR THE LIST OF PROPERTIES THAT YOU PROVIDE

def rooms(room_ids):
    property_data = []
    for room_id in room_ids:
        prop_data = vasilis_scrape_property(room_id)
        #print(type(prop_data))
        if type(prop_data)!=type('str'):
            prop_data.pop('Amenities', None)
            prop_data = flatten(prop_data)
            prop_data['capacity'] = prop_data.pop('Accommodates:')
            prop_data['bathrooms'] = prop_data.pop('Bathrooms:')
            prop_data['bedrooms'] = prop_data.pop('Bedrooms:')
            prop_data['beds'] = prop_data.pop('Beds:')
            prop_data['property_type'] = prop_data.pop('Property type:')
            prop_data['min_stay'] = prop_data.pop('min_stay_nights')
            prop_data['accuracy_10'] = prop_data.pop('rating_breakdown_Accuracy')
            prop_data['check_in_10'] = prop_data.pop('rating_breakdown_Check In')
            prop_data['cleanliness_10'] = prop_data.pop('rating_breakdown_Cleanliness')
            prop_data['communication_10'] = prop_data.pop('rating_breakdown_Communication')
            prop_data['location_10'] = prop_data.pop('rating_breakdown_Location')
            prop_data['value_10'] = prop_data.pop('rating_breakdown_Value')
            property_data = property_data + [prop_data]
    property_df = pd.DataFrame.from_dict(property_data)
    return property_df

## THIS CODE CHUNK ESSENTIALLY GETS ALL THE USER ID DATA BASED ON THE ENETERED LIST OF USER id_onwards

def vasilis_get_user_info(user_id):
    url = "https://www.airbnb.co.uk/users/show/"+str(user_id)
    res = requests.get(url)
    page_source = res.text

    # Part B
    tags = re.findall(r'<dt>.*?</dt>', page_source)
    tags = [re.sub('<dt>', '', i) for i in tags]
    tags = [re.sub('</dt>', '', i) for i in tags]
    content = re.findall(r'<dd>.*?</dd>', page_source)
    content = [re.sub('<dd>', '', i) for i in content]
    content = [re.sub('</dd>', '', i) for i in content]
    master_dict = dict(zip(tags,content))

    # Part A
    soup = BeautifulSoup(page_source)
    a = soup.find_all('img',class_='img-responsive')
    s =[i.get('src') for i in a]
    try:
        s.remove('https://a0.muscache.com/airbnb/static/profile/symbol-empty-state-d97ee7a003fdab31cfaa9b20bd3a27ff.png')
        prof_pic = list(set(s))[0]
        master_dict['pro_pic_url'] = prof_pic
    except ValueError:
        prof_pic = list(set(s))[0]
        master_dict['pro_pic_url'] = prof_pic



    #Part C
    res = soup.find_all('div', class_='panel space-4')
    verified_info = []
    for item in res:
        if item.find('div', class_='panel-header').contents[0].strip()=='Verified info':
            verified_info = [i.contents[0].strip() for i in item.find_all('div', class_='col-12 col-middle')]
    master_dict['verified_info'] = verified_info

    #Part D
    res = soup.find_all('div', class_='panel space-4')
    connected_accounts = []
    for item in res:
        #print(item.find('div', class_='panel-header').contents[0].strip())
        if item.find('div', class_='panel-header').contents[0].strip()=='Connected accounts':
            connected_accounts = [i.contents[0].strip() for i in item.find_all('div', class_='col-12 col-middle')]

    master_dict['connected_accounts'] = connected_accounts

    #Part E
    lst = soup.find_all('div', class_="reviews row-space-4")
    n_reviews = 0
    for l in lst:
        if l.get('id')=='reviews':
            n_reviews = l.find('h2').find('small').contents[0][1:-1]
    master_dict['no_reviews'] = n_reviews

    # Part  F
    description = ""
    for item in soup.find_all("div", class_="space-top-2"):
        tmp = item.find_all('p')
        if len(tmp)>0:
            try:
                lst = [t.contents[0] for t in tmp]
                break
            except IndexError:
                lst = []
    description = ' '.join(lst)
    description = re.sub('\"','',description)
    master_dict['description']= description

    # Part G
    master_dict['host_name'] = soup.find('img', src=master_dict['pro_pic_url']).get('alt')

    #Part H
    location = soup.find('div',class_="h5 space-top-2").find('a').contents[0]
    joined = soup.find('span', class_="text-normal").contents[0].strip()
    year = re.search(r'[0-9]{4}',joined).group(0)
    month = re.search(r'(January)|(February)|(March)|(April)|(May)|(June)|(July)|(August)|(September)|(October)|(November)|(December)',joined).group(0)
    master_dict['joined'] = str(month)+" "+str(year)
    return master_dict


## ITERATES OVER A LIST OF HOST NAMES AND GENERATES A TABLE OF POTENTIAL HOSTS TO BE CONTACTED

def users(user_ids):
    if type(user_ids)==type(1):
       user_ids = [user_ids]
    all_user_data = []
    for user_id in user_ids:
        temp_data = vasilis_get_user_info(user_id)
        try:
            temp_data['education'] = temp_data.pop('School')
        except KeyError:
            temp_data['education'] = None
        try:
            temp_data['work'] = temp_data.pop('Work')
        except KeyError:
            temp_data['work'] = None
        temp_data['accounts'] = temp_data.pop('connected_accounts')
        temp_data['name'] = temp_data.pop('host_name')
        temp_data['reviews'] = temp_data.pop('no_reviews')
        temp_data['profile_pic'] = temp_data.pop('pro_pic_url')
        temp_data['verifications'] = temp_data.pop('verified_info')
        all_user_data = all_user_data + [temp_data]
    return pd.DataFrame.from_dict(all_user_data)

## THIS CHUNK OF CODE IS THE SENTIMENT ANALYSIS LIBRARY FOR A GIVEN SET OF PROPERTIES

def get_comments(room_id):
    """
    a) Find total number of reviews from reconstructed url
    b) Iterate though all comments and accumulate them in a df
    c) Apply sentiment analysis
    """

    offset=0

    url_part1 = "https://www.airbnb.co.uk/api/v2/reviews?key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=GBP&locale=en-GB&listing_id="
    url_part2 = "&role=guest&_format=for_p3&_limit=50&_offset="
    url_part3 = "&_order=language"

    columns = ['comments', 'localized_date', 'reviewer_id', 'reviewee_id', 'reviewer_first_name',
                   'reviewer_profile_path']


    #function to detect language from a comment
    def detect_language(review):
        try:
            try:
                return detect(review)
            except lang_detect_exception:
                return "na"
        except TypeError:
            return "na"

    def helper(sample):
        s = flatten(sample)
        keep = { your_key: s[your_key] for your_key in columns }

        return pd.DataFrame.from_dict(data=keep, orient='index').transpose()

    def process_sample(sample):
        tmp = pd.DataFrame(columns=columns)
        for i in range(len(sample['reviews'])):
            tmp = tmp.append(helper(sample['reviews'][i]))
        return tmp


    #Part A: Find total number of comments for property

    url = url_part1+str(room_id)+url_part2+str(offset)+url_part3
    r = requests.get(url)

    no_comments = int(flatten(json.loads(r.text))['metadata_reviews_count'])

    print("Number of reviews for this property is: ",no_comments)

    #Part B: Get Make a Data Frame with comment, date, reviewer_id, reviewer_name, reviewer profile path

    results = [] #contains all dicts which contain 50 reviews each
    for offset in range(0, (no_comments//50)+1):
        url = url_part1+str(room_id)+url_part2+str(offset*50)+url_part3
        r = requests.get(url)
        results.append(json.loads(r.text))

    # initialize empty dataframe
    df = pd.DataFrame(columns=columns)

    #process 50 results at a time and add to a dataframe
    for r in results:
        df = df.append(process_sample(r))

    df['lang'] = df.comments.apply(detect_language)
    df = df[df['lang']=='en']
    df.drop('lang', axis=1,inplace=True)

    #Part C: Sentiment Analysis

    sid = SentimentIntensityAnalyzer()
    score = []
    for sentence in df['comments']:
        ss = sid.polarity_scores(sentence)
        testimonial = TextBlob(sentence)
        score.append(np.mean([ss['pos'] - ss['neg'], testimonial.polarity]))
    df['score'] = score
    return df

## THIS CHUNK OF CODE WILL ESSENTIALLY HAMMER OUT ALL SENTIMENT DATA BY ROOM ID
## WITH NUMBER OF COMMENT, DATAFRAME OF SENTIMENT ANALYSIS FORMAT

def sentiment(rooms_ids):
    all_sentiment = []
    for room_id in room_ids:
        com = get_comments(room_id)
        all_sentiment = all_sentiment + [com]
    return all_sentiment

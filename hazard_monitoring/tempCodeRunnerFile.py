from datetime import date,datetime
import pandas as pd
import urllib.request
import json

api_url = "http://www.gdacs.org/export.aspx?profile=ARCHIVE&type=geojson&eventtype=EQ,TS,TC,FL,VO,DR&from={}&to={}"
start_date = date(2017, 1, 1)
end_date = date(2019, 8, 22)
daterange = pd.date_range(start_date, end_date)

df = pd.DataFrame(columns=['Title', 'Summary', 'Id', 'Copyright', 'PublishedDate', 'LastUpdate', 'Category', 'Author', 'Content', 'LinkURl', '_georss_feature_number'
                           'gdacs_country', 'gdacs_cap', 'gdacs_alertlevel', 'gdacs_episodeid', 'gdacs_eventid', 'gdacs_eventname', 'gdacs_eventtype', 'gdacs_fromdate', 'gdacs_gtslink',
                           'gdacs_population', 'gdacs_severity', 'gdacs_todate', 'gdacs_version', 'gdacs_vulnerability', 'gdacs_year', 'gdacs_glide', '_x', '_y', '_z'])

for single_date in daterange:
    single_date = single_date.strftime("%Y-%m-%d")
    print(single_date)
    api_call = api_url.format(single_date, single_date)
    with urllib.request.urlopen(api_call) as url:
        data = json.loads(url.read().decode())
        data = data['features']
        for feature in data:
            properties = feature['properties']
            gdacs_event = dict()
        #     print(feature)
            # print(properties['fromdate'])
            fromdate=datetime.strptime(properties['fromdate'], '%d/%b/%Y %H:%M:%S')
            todate=datetime.strptime(properties['todate'], '%d/%b/%Y %H:%M:%S')
            # print(fromdate.strftime("%Y-%m-%d"))
            gdacs_event['Title'] = properties['htmldescription']
            gdacs_event['Id'] = '{}{}'.format(properties['eventtype'], properties['eventid'])
            gdacs_event['IdllLinkURl'] = properties['link']
            gdacs_event['gdacs_country'] = properties['countrylist']
            gdacs_event['gdacs_alert_level'] = properties['alertlevel']
            gdacs_event['gdacs_episodeid'] = properties['episodeid']
            gdacs_event['gdacs_eventid'] = properties['eventid']
            gdacs_event['gdacs_eventname'] = properties['eventname']
            gdacs_event['gdacs_eventtype'] = properties['eventtype']
            gdacs_event['gdacs_fromdate'] = fromdate.strftime("%Y-%m-%d")
            gdacs_event['gdacs_todate'] = todate.strftime("%Y-%m-%d")
            gdacs_event['_x'] = feature['geometry']['coordinates'][0]
            gdacs_event['_y'] = feature['geometry']['coordinates'][1]
           # remove old entries and get uupdates
            df = df[df.gdacs_eventid != gdacs_event['gdacs_eventid']]
            df = df.append(gdacs_event, ignore_index=True)
    # break

#     print(api_call)
#     print(df)
    # print(single_date)
    # print(data)
df.to_excel('hazard_monitoring/Historical_GDACS_data.xlsx')

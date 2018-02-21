import os
import httplib2
import argparse
import time
import pypyodbc

from datetime import timedelta
from googleapiclient import errors
from googleapiclient import discovery
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from fuzzywuzzy import fuzz


scope = 'https://www.googleapis.com/auth/webmasters.readonly'
row_limit = 5000


def establish_database_connection():
    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

def get_date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def request_api_dimensions(start_row, start_date, end_date, dimensions_list):

    return {"startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions_list,
            "rowLimit": row_limit,
            "startRow": start_row,
            }


def request_api_dimensions_by_device(start_row, start_date, end_date, device, dimensions_list):

    return {"startDate": start_date,
            "endDate": end_date,
            "dimensions": dimensions_list,
            "rowLimit": row_limit,
            "startRow": start_row,
            "dimensionFilterGroups": [{"filters": [{"dimension": "device", "expression": str(device)}]}]
            }


def query_classification(branding_list, single_query):
    classification_dict = {'branding': 'Not Branded', 'local': 0, 'question': 0}

    brand_probability = 0
    for branded_name in branding_list:
        temp_probability = fuzz.token_set_ratio(str(branded_name).lower(), str(single_query).lower())
        if temp_probability > brand_probability:
            brand_probability = temp_probability

    if brand_probability >= 75:
        classification_dict['branding'] = 'Branded'
    else:
        classification_dict['branding'] = 'Not Branded'

    query_word_list = str(single_query).split()

    for x in query_word_list:
        if x.lower() in ['near', 'hour', 'hours', 'location', 'open', 'mall', 'today',
                         'close', 'closest', 'closed', 'area', 'service', 'my', 'store', 'atm', 'branch']:
            classification_dict['local'] = 1

    for x in query_word_list:
        if x.lower() in ['who', 'what', 'where', 'when', 'how', 'why', 'mean',
                         'meaning', 'represent', 'chart', 'guide']:
            classification_dict['question'] = 1

    return [classification_dict['branding'], classification_dict['local'], classification_dict['question']]


def execute_request(service, request, property_url):
    """
    Executes a searchAnalytics.query request.

    Args:
        service: The webmasters service to use when executing the query.
        request: The request to be executed.
        property_url: the url in search console that you want to pull data from

    Returns:
        An array of response rows.
    """
    api_query_request = errors.HttpError

    for i in range(0, 15):
        while True:
            try:
                api_query_request = service.searchanalytics().query(siteUrl=property_url, body=request).execute()
                print request
                break
            except errors.HttpError:
                print 'Http Error Exception:', i, 'Second Delay'
                time.sleep(i)
                continue
        break

    return api_query_request


def google_service(argv, name, version, doc, filename, parents, discovery_filename=None):

    parent_parsers = [tools.argparser]
    parent_parsers.extend(parents)
    parser = argparse.ArgumentParser(
      description=doc,
      formatter_class=argparse.RawDescriptionHelpFormatter,
      parents=parent_parsers)
    flags = parser.parse_args(argv[1:])

    client_secrets = os.path.join(os.path.dirname(filename), 'client_secrets.json')
    flow = client.flow_from_clientsecrets(client_secrets, scope=scope, message=tools.message_if_missing(client_secrets))

    storage = file.Storage(name + '.dat')
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)

    http = credentials.authorize(http=httplib2.Http())

    if discovery_filename is None:
        # Construct a service object via the discovery service.
        service = discovery.build(name, version, http=http)
    else:
        # Construct a service object using a local discovery document file.
        with open(discovery_filename) as discovery_file:
            service = discovery.build_from_document(discovery_file.read(),
                                                    base='https://www.googleapis.com/',
                                                    http=http)

    return service, flags


import httplib2
import sys
import pypyodbc
import copy
import datetime
import time
import traceback

from datetime import timedelta, date
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

reload(sys)
sys.setdefaultencoding('utf-8')


def get_service(api_name, api_version, scope, key_file_location, service_account_email):
    """
    Get a service that communicates to a Google API.
    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scope: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account p12 key file.
        service_account_email: The service account email address.

    Returns:
        A service that is connected to the specified API.
    """
    credentials = ServiceAccountCredentials.from_p12_keyfile(service_account_email, key_file_location, scopes=scope)
    http = credentials.authorize(httplib2.Http())
    service = build(api_name, api_version, http=http)  # Build the service object.
    return service


def get_profile_id(service, ga_account_profile):
    # Use the Analytics service object to get the profile id.

    account_and_profile_list = str(ga_account_profile).split('=')
    property_id = account_and_profile_list[0]

    account_id_and_view = account_and_profile_list[0].split('-')
    account_id = account_id_and_view[1]
    view_id = account_and_profile_list[1]

    profiles = service.management().profiles().list(accountId=account_id, webPropertyId=property_id).execute()

    for profile in profiles.get('items'):

        if view_id == profile.get('id'):
            print 'Name:', profile.get('name')
            return profile.get('id')

    return None


def ga_query(service, profile_id, page_index, start_date, end_date, metrics, dimensions, filters, segments):

    # Google Analytics metrics (max of 10 metrics can be requested)
    # metrics = 'ga:sessions, ga:bounces, ga:goalCompletionsAll, ga:pageviews, ga:timeOnPage, ga:transactions, ga:transactionRevenue'

    # Google Analytics metrics (max of 7 dimensions can be requested)
    # dimensions = 'ga:date, ga:deviceCategory, ga:landingPagePath'

    if filters != '0' and segments != '0':
        return service.data().ga().get(ids='ga:' + profile_id,
                                       start_date=start_date,
                                       end_date=end_date,
                                       metrics=metrics,
                                       dimensions=dimensions,
                                       filters=filters,  # filters='ga:source==google;ga:medium==organic'
                                       segment=segments,  # segment=sessions::condition::ga:medium%3D%3Dreferral
                                       samplingLevel='HIGHER_PRECISION',
                                       start_index=str(page_index + 1),
                                       max_results=str(page_index + 10000)).execute()

    elif filters == '0' and segments != '0':

        return service.data().ga().get(ids='ga:' + profile_id,
                                       start_date=start_date,
                                       end_date=end_date,
                                       metrics=metrics,
                                       dimensions=dimensions,
                                       segment=segments,  # segment=sessions::condition::ga:medium%3D%3Dreferral
                                       samplingLevel='HIGHER_PRECISION',
                                       start_index=str(page_index + 1),
                                       max_results=str(page_index + 10000)).execute()
    elif filters != '0' and segments == '0':

        return service.data().ga().get(ids='ga:' + profile_id,
                                       start_date=start_date,
                                       end_date=end_date,
                                       metrics=metrics,
                                       dimensions=dimensions,
                                       filters=filters,  # filters='ga:source==google;ga:medium==organic'
                                       samplingLevel='HIGHER_PRECISION',
                                       start_index=str(page_index + 1),
                                       max_results=str(page_index + 10000)).execute()

    else:

        return service.data().ga().get(ids='ga:' + profile_id,
                                       start_date=start_date,
                                       end_date=end_date,
                                       metrics=metrics,
                                       dimensions=dimensions,
                                       samplingLevel='HIGHER_PRECISION',
                                       start_index=str(page_index + 1),
                                       max_results=str(page_index + 10000)).execute()


def get_results(service, profile_id, start_date, end_date, site_url, clientID, metrics, dimensions, filters, segments, sql_table_name):

    api_query = ga_query(service, profile_id, 0, start_date, end_date, metrics, dimensions, filters, segments)

    query = api_query.get('query')
    for key, value in query.iteritems():
        print '%s = %s' % (key, value)

    num_of_results = api_query.get('totalResults')
    print "Total Rows: ", num_of_results

    count = 0
    for page_index in xrange(0, num_of_results, 10000):
        query_results = ga_query(service, profile_id, page_index, start_date, end_date, metrics, dimensions, filters, segments)
        count += write_to_sql(query_results, site_url, clientID, start_date, sql_table_name, metrics, dimensions)
        print 'Total Rows Imported: ', count


def print_account_data(service):
    # Get all account associated to this google service account
    accounts = service.management().accounts().list().execute()

    for someAccount in accounts.get('items'):
        for data in someAccount:
            print data, ':\t', someAccount[data]
        print '---------------------------------------------------------------------------------------------------'


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


def get_date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def get_list_of_clients():

    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    SQLCommand = ("SELECT name, locationID, googleProdID, serviceAccount, url, googleAccountProfile, "
                  "GA_last_pull_date, GA_stop_pull_date, sql_table_name, GA_recurrence"
                  " FROM clientID_list_GA_custom ")

    cursor.execute(SQLCommand)

    properties_list = []
    row = cursor.fetchone()

    while row is not None:
        row_list = list(row)
        properties_list.append(row_list)
        row = cursor.fetchone()

    connection.commit()  # Save to Server
    connection.close()  # Stop Connection to Server

    return properties_list


def get_list_of_ga_tables():

    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    SQLCommand = ("SELECT sql_table_name, metrics, dimensions, filters, segments FROM custom_GA_table_list")
    cursor.execute(SQLCommand)

    ga_table_dict = dict()
    row = cursor.fetchone()

    while row is not None:
        row_list = list(row)

        ga_table_dict[row_list[0]] = {
                                        "metrics": row_list[1],
                                        "dimensions": row_list[2],
                                        "filters": row_list[3],
                                        "segments": row_list[4]
                                      }
        row = cursor.fetchone()

    connection.commit()  # Save to Server
    connection.close()  # Stop Connection to Server

    return ga_table_dict


def main():
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']  # Read and Analyze Data

    properties_list = get_list_of_clients()
    ga_table_dict = get_list_of_ga_tables()

    for single_prop in properties_list:

        client_name = single_prop[0]
        clientID = single_prop[1]
        key_file_location = single_prop[2]
        service_account_email = single_prop[3]
        site_url = single_prop[4]
        GA_account_profile = single_prop[5].strip()
        GA_last_pull_date = single_prop[6]
        GA_stop_pull_date = single_prop[7]
        sql_table_name = single_prop[8]
        GA_recurrence = single_prop[9]

        metrics = ga_table_dict[sql_table_name]["metrics"]
        dimensions = ga_table_dict[sql_table_name]["dimensions"]
        filters = ga_table_dict[sql_table_name]["filters"]
        segments = ga_table_dict[sql_table_name]["segments"]

        # Ensure that google prod id file name has correct ending file extension
        if not key_file_location.endswith(".p12"):
            key_file_location += ".p12"

        # Used to Remove '/' from end of URL
        if site_url.endswith("/"):
            site_url = site_url[0:len(site_url) - 1]

        print 'client_name:', client_name
        print 'clientID:', clientID
        print 'key_file_location:', key_file_location
        print 'service_account_email:', service_account_email
        print 'site_url:', site_url
        print 'GA_account_profile:', GA_account_profile
        print 'GA_last_pull_date:', GA_last_pull_date
        print 'sql_table_name', sql_table_name
        print 'metrics', metrics
        print 'dimensions', dimensions
        print 'filters', filters
        print 'segments', segments

        # Define full path of location for google prod ids
        key_file_location = "\AutomationProjects\prodID\\" + key_file_location.lower()

        # Authenticate and construct service.
        service = get_service('analytics', 'v3', scope, key_file_location, service_account_email)

        # print_account_data(service)

        if GA_account_profile is not None and sql_table_name is not None:

            single_date_string = None

            try:
                start_date_list = GA_last_pull_date.split('-')
                extraction_start_date = date(int(start_date_list[0]), int(start_date_list[1]), int(start_date_list[2]))

                current_date_string = time.strftime("%Y-%m-%d")
                current_date_list = current_date_string.split('-')
                extraction_end_date = date(int(current_date_list[0]), int(current_date_list[1]), int(current_date_list[2]))

                for single_date in get_date_range(extraction_start_date + timedelta(days=1), extraction_end_date):
                    single_date_string = single_date.strftime("%Y-%m-%d")
                    print single_date_string

                    profile = get_profile_id(service, GA_account_profile)
                    get_results(service, profile, single_date_string, single_date_string, site_url, clientID,
                                metrics, dimensions, filters, segments, sql_table_name)

                update_client_list_with_latest_ga_pull_date(sql_table_name, clientID) # Update latest pull date

            except Exception as e:
                print 'ERROR:', e
                traceback.print_exc()

                if single_date_string is None:
                    update_extractor_log(clientID, time.strftime("%Y-%m-%d"), 0, sql_table_name, 'Error')
                else:
                    update_extractor_log(clientID, single_date_string, 0, sql_table_name, 'Error')

        print '---------------------------------------------------------------------------------------------------'

if __name__ == '__main__':
    main()

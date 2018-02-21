import requests
import json
import traceback
import pypyodbc
import time
import sys
import datetime

from isoweek import Week

reload(sys)
sys.setdefaultencoding('utf-8')


def main():

    properties_list = get_list_of_clients()

    for single_prop in properties_list:

        clientID = single_prop[0]  # clientId is locationID
        account_id = single_prop[1]
        last_pull_date = single_prop[2] # currently in YYYY-MM-DD format
        sql_table_name = single_prop[3]
        json_file_name = single_prop[4]
        username = single_prop[5]
        password = single_prop[6]
        time_frequency = single_prop[7]

        # generate a json query request based on last full week/month
        query_request = get_correct_post_request(json_file_name, time_frequency, last_pull_date)

        print 'Query Extraction Range:', query_request["filter"]

        # get total_num_rows
        json_object = get_bright_edge_data(account_id, username, password, query_request)

        if json_object is not None:
            total_num_rows = json_object['total']
            print total_num_rows

            if total_num_rows > 0:

                print 'Total Number of Rows json_object[\"total\"]:', total_num_rows

                # pagination to get all rows from query request
                for start_offset in range(0, total_num_rows, 1000):

                    query_request["offset"] = start_offset

                    json_object = get_bright_edge_data(account_id, username, password, query_request)

                    print 'Total Number of Rows json_object[\"values\"]:', len(json_object["values"])

                    write_to_sql_table(query_request, json_object["values"], account_id, clientID, sql_table_name, time_frequency)

                update_client_list_with_latest_bright_edge_pull_date(clientID, account_id, sql_table_name)

            else:
                print 'ROWS FOUND', total_num_rows, ' | NO DATA'
        else:
            print 'json return object from POST request is NULL'


def get_correct_post_request(json_file_name, time_frequency, last_pull_date):

    if time_frequency == 'weekly':

        # parse last_pull_date to ISO format YYYYWW
        last_extracted_date = last_pull_date.split('-')
        iso_date_format = datetime.date(int(last_extracted_date[0]),
                                        int(last_extracted_date[1]),
                                        int(last_extracted_date[2])).isocalendar()

        last_pull_week = str(iso_date_format[0])

        if int(iso_date_format[1]) < 10:
            last_pull_week += "0"

        last_pull_week += str(iso_date_format[1])

        # generate a json query request based on weekly data
        query_request = json_post_request(json_file_name, last_pull_week, get_last_full_week())
    else:

        # parse last_pull_date to YYYYMM format
        last_extracted_date = last_pull_date.split('-')

        last_pull_month = str(last_extracted_date[0])

        if int(last_extracted_date[1]) < 10:
            last_pull_month += "0"

        last_pull_month += str(last_extracted_date[1])

        # generate a json query request based on monthly data
        query_request = json_post_request(json_file_name, last_pull_month, get_last_full_month())

    return query_request


def json_post_request(json_file_name, start_period, end_period):

    full_path = 'AutomationProjects\BrightEdgeExtractor\BrightEdge_Querys\\' + json_file_name

    query_request = json.loads(open(full_path).read())

    query_request["filter"].append(["time", "gt", start_period])
    query_request["filter"].append(["time", "le", end_period])

    return query_request


def get_last_full_month():
    today = datetime.date.today()
    current_month_first_day = today.replace(day=1)  # the first date of the current month
    last_month_last_day = current_month_first_day - datetime.timedelta(days=1)

    return last_month_last_day.strftime("%Y%m")  # YYYYMM format


def get_last_full_week():
    last_week = Week.thisweek() - 1

    if int(last_week.week) < 10:
        return str(last_week.year) + "0" + str(last_week.week)

    return str(last_week.year) + str(last_week.week)  # YYYYWW iso week format


def write_to_sql_table(query_request, results, account_id, clientID, sql_table_name, time_frequency):

    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    dimensions_list = query_request["dimension"]
    measures_list = query_request["measures"]

    dimensions_list.sort()
    measures_list.sort()

    print "dimensions_list order:", dimensions_list
    print "measures_list: ", measures_list

    # SQL INSERT data command
    SQLCommand = create_sql_insert_command(sql_table_name, dimensions_list, measures_list)

    count = 0
    for row_list in results:

        sql_values = []

        for dimension in dimensions_list:

            if dimension == "time":
                formatted_time = format_query_time(time_frequency, row_list[dimension])
                sql_values.append(formatted_time)
            else:
                sql_values.append(row_list[dimension])

        for measure in measures_list:
            sql_values.append(row_list[measure])

        sql_values.insert(0, account_id)
        sql_values.insert(0, time.strftime("%Y-%m-%d"))
        sql_values.insert(0, 'auto')
        sql_values.insert(0, clientID)

        # Execute SQL command execute(SQLCommand, Values)
        cursor.execute(SQLCommand, sql_values)
        count += 1

    connection.commit()  # Save to Server
    connection.close()  # Stop Connection to Server

    print "\n\nData Loaded to Database Successfully: Rows Imported", count

    return count


def format_query_time(time_frequency, raw_time):

    iso_time = str(raw_time)  # YYYYWW or YYYYMM
    year = iso_time[:4]
    week = iso_time[4:]

    if time_frequency == 'weekly':
        formatted_time = str(Week(int(year), int(week)).monday())  # YYYY-MM-DD
    else:
        formatted_time = year + "-" + week + "-01"  # YYYY-MM-01

    return formatted_time


def create_sql_insert_command(sql_table_name, dimensions_list, measures_list):

    number_of_columns = len(dimensions_list) + len(measures_list)
    column_name_query = ""

    for dimension in dimensions_list:
        if dimension == "search_engine:id[0]":
            column_name_query += "search_engine_device_id, "
        elif dimension == "search_engine[0]":
            column_name_query += "search_engine_device, "
        elif dimension == "domain:raw_name":
            column_name_query += "domain_raw_name, "
        else:
            column_name_query += dimension + ", "

    column_name_query += ', '.join(measure for measure in measures_list)

    bind_place_holders = "?, " * number_of_columns + "?, ?, ?, ?"

    # SQL INSERT data command
    return ("INSERT INTO " + sql_table_name + " (clientID, input_type, timeStamp, account_id, "
                  + column_name_query.strip(", ") + ") VALUES (" + bind_place_holders + ")")


def update_client_list_with_latest_bright_edge_pull_date(location_id, account_id, sql_table_name):

    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    brightEdge_last_pull_date = None

    try:

        SQLCommand = ("SELECT DISTINCT TOP 1 time FROM " + sql_table_name
                      + " WHERE clientID = ? AND account_id = ? ORDER BY time DESC")

        cursor.execute(SQLCommand, [location_id, account_id])
        brightEdge_last_pull_date = list(cursor.fetchone())[0]

    except Exception:
        print 'ERROR update_client_list_with_latest_bright_edge_pull_date()'

    if brightEdge_last_pull_date is not None:
        SQLCommand = ("UPDATE clientID_list_BrightEdge_custom "
                      "SET brightEdge_last_pull_date = ? "
                      "WHERE locationID = ? AND account_id = ? AND sql_table_name = ?")
        cursor.execute(SQLCommand, [brightEdge_last_pull_date, location_id, account_id, sql_table_name])

        print 'CLIENT ID:', location_id, 'UPDATED BRIGHT_EDGE_LAST_PULL_DATE TO', brightEdge_last_pull_date

    connection.commit()  # Save to Server
    connection.close()  # Stop Connection to Server


def get_list_of_clients():

    # Create reference connection to SQL Server
    connection = pypyodbc.connect('Driver=########;'
                                  'Server=########;'
                                  'Database=#########;'
                                  'uid=#########;'
                                  'pwd=###########')

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    SQLCommand = ("SELECT [locationID], [account_id], [brightEdge_last_pull_date], [sql_table_name], "
                  "[json_file_name], [api_username], [api_password], [extraction_frequency] "
                  "FROM [clientID_list_BrightEdge_custom]")

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


def get_bright_edge_data(account_id, username, password, query_request):

    try:

        url = "https://api.brightedge.com/3.0/query/" + str(account_id)

        query_request_string = "query=" + json.dumps(query_request)

        response = requests.request("POST", url, data=query_request_string, auth=(username, password))
        json_object = response.json()

        # print_response_headers(response)
        # print json.dumps(json_object, indent=4, sort_keys=True)

        return json_object

    except Exception as error:
        print error

    return None


def view_accounts(username, password):
    response = requests.get('https://api.brightedge.com/3.0/objects/accounts', auth=(username, password))

    json_object = response.json()

    print_response_headers(response)
    print json.dumps(json_object, indent=4, sort_keys=True)
    print "Number of Accounts in JSON List:", len(json_object['accounts'])


def view_search_engine_details(username, password, account_id):

    get_request_url = 'https://api.brightedge.com/3.0/objects/searchengines/' + str(account_id)
    response = requests.get(get_request_url, auth=(username, password))

    json_object = response.json()

    print_response_headers(response)
    print json.dumps(json_object, indent=4, sort_keys=True)


def print_response_headers(response):
    print 'HTTP Code:', response.status_code
    print 'Headers:', response.headers
    print 'Request:', response.request
    print 'Response Object:', response


if __name__ == '__main__':
    main()

import sys
import argparse
import time
import GoogleAPI_SC_Extractor as SC

from datetime import timedelta, date

reload(sys)
sys.setdefaultencoding('utf-8')

property_url = ''
clientID = 0
branded_name_list = []

scope = 'https://www.googleapis.com/auth/webmasters.readonly'
row_limit = 5000


def main(argv):

    properties_list = get_list_of_clients_for_SC()
    for single_prop in properties_list:

        SC_last_pull_date = single_prop[3]

        global property_url
        property_url = single_prop[2]

        global clientID
        clientID = single_prop[1]

        global branded_name_list
        branded_name_list = [str(single_prop[0]).lower(), single_prop[4]]

        print 'clientID: ', clientID
        print 'SC_last_pull_date:', SC_last_pull_date
        print 'property_url:', property_url
        print 'branded_name_list:', branded_name_list, '\n'

        start_date_list = SC_last_pull_date.split('-')
        extraction_start_date = date(int(start_date_list[0]), int(start_date_list[1]), int(start_date_list[2]))

        current_date_string = time.strftime("%Y-%m-%d")
        current_date_list = current_date_string.split('-')
        extraction_end_date = date(int(current_date_list[0]), int(current_date_list[1]), int(current_date_list[2]))

        for single_date in SC.get_date_range(extraction_start_date + timedelta(days=1), extraction_end_date):
            single_date_string = single_date.strftime("%Y-%m-%d")
            print single_date_string
            get_data_by_pagination(argv, single_date_string, single_date_string)

        clean_data()
        move_from_temp_to_final_table()

        update_client_list_with_latest_sc_pull_date(clientID)
        print '####################################################################################################'

    print 'DONE'


def get_data_by_pagination(argv, start_date, end_date):
    parser = argparse.ArgumentParser(add_help=False)

    parser.add_argument('property_uri', type=str, nargs='?', const=property_url, help="URL")
    parser.add_argument('start_date', type=str, nargs='?', const=start_date, help="YYYY-MM-DD")
    parser.add_argument('end_date', type=str, nargs='?', const=end_date, help="YYYY-MM-DD")

    service, flag = SC.google_service(argv, 'webmasters', 'v3', __doc__, __file__, parents=[parser])

    get_data_for_sc_api_table_pages(service, start_date, end_date)


def get_list_of_clients_for_SC():

    # Create reference connection to SQL Server
    connection = SC.establish_database_connection()

    # Create SQL pointer object for python to SQL command execution
    cursor = connection.cursor()

    SQLCommand = ("SELECT name, locationID, searchConsoleURL, SC_last_pull_date, additional_branded_keyword"
                  " FROM clientID_list_SC_custom")

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


def get_data_for_sc_api_table_pages(service, start_date, end_date):
    start_row = 0

    while True:

        api_request = SC.request_api_dimensions(start_row, start_date, end_date, ["page"])
        query_results = SC.execute_request(service, api_request, property_url)

        if 'rows' in query_results:
            start_row += write_to_sc_api_table_pages(start_date, query_results)
            print 'Pages Table: Count/Start Row:', start_row
            print '----------------------------------------'

        if 'rows' not in query_results:
            break

    print 'API Pull Complete: Rows Imported: sc_api_table_pages:', start_row
    print '=================================================================================================='


def write_to_sc_api_table_pages(start_date, query_results):
    # Create reference connection to SQL Server
    connection = SC.establish_database_connection()

    cursor = connection.cursor()

    sql_command = ("INSERT INTO custom_sc_api_table_pages"
                   "(clientID, input_type, timeStamp, "
                   "date, pagepath, clicks, impressions, ctr, position) "
                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")

    count = 0

    for row in query_results["rows"]:
        row_list = [start_date]

        for dimensions in row["keys"]:
            row_list.append(dimensions)

        row_list.extend([row["clicks"], row["impressions"], row["ctr"], row["position"]])

        row_list.insert(0, time.strftime("%Y-%m-%d"))
        row_list.insert(0, 'auto')
        row_list.insert(0, clientID)

        cursor.execute(sql_command, row_list)  # Execute SQL command execute(SQLCommand, Values)

        count += 1

    connection.commit()   # Save to Server
    connection.close()   # Stop Connection to Server

    print "Data Loaded to Database Successfully with", count, 'Rows of Data'
    SC.update_extractor_log(clientID, start_date, count, 'custom_sc_api_table_pages', 'Complete')

    return count


if __name__ == '__main__':
    main(sys.argv)

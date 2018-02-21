import httplib2
import argparse
import os
import sys
import json
import openpyxl

from googleapiclient import discovery
from oauth2client import client
from oauth2client import file
from oauth2client import tools


def google_service(argv, name, version, doc, filename, parents, discovery_filename=None):
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/webmasters.readonly']  # Read and Analyze Data

    parent_parsers = [tools.argparser]
    parent_parsers.extend(parents)
    parser = argparse.ArgumentParser(description=doc,
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


def main(argv):

    target_url = 'property_name'

    # for command line arguments
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('property_uri', type=str, nargs='?', const=target_url, help="URL")

    # generate google analytics service
    service, flag = google_service(argv, 'webmasters', 'v3', __doc__, __file__, parents=[parser])

    category = ["authPermissions", "notFollowed", "notFound", "other", "serverError", "soft404"]

    output_dict = {}
    for error in category:
        print "Error:", error
        json_object = service.urlcrawlerrorssamples().list(siteUrl=target_url, category=error, platform='web').execute()

        # print json.dumps(json_object, indent=4)
        output_dict[error] = json_object

    export_to_excel(output_dict)


def export_to_excel(output_dict):
    work_book = openpyxl.Workbook()
    work_sheet = work_book.active

    work_sheet.append(['Error Category', 'First Detected', 'Last Crawled', 'PageType', 'pageUrl'])

    for error_key, value in output_dict.items():
        print error_key
        if 'urlCrawlErrorSample' in value.keys():
            for item in value['urlCrawlErrorSample']:
                print item
                page_type = item['pageUrl'].split('/')[0]
                work_sheet.append([error_key, item['first_detected'], item['last_crawled'], page_type, item['pageUrl']])

    work_book.save("search_console_crawl_error_output.xlsx")


if __name__ == '__main__':
    main(sys.argv)

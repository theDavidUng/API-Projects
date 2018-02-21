import httplib2
import argparse
import os
import sys
import json
from googleapiclient import discovery
from oauth2client import client
from oauth2client import file
from oauth2client import tools


def google_service(argv, name, version, doc, filename, parents, discovery_filename=None):
    # Define the auth scopes to request.
    scope = ['https://www.googleapis.com/auth/analytics.readonly']  # Read and Analyze Data

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

    # for command line arguments
    parser = argparse.ArgumentParser(add_help=False)

    # generate google analytics service
    service, flag = google_service(argv, 'analytics', 'v3', __doc__, __file__, parents=[parser])

    # JSON object that contains all segments available under the user gsitemap email
    segments = service.management().segments().list().execute()

    print "Username:", segments.get('username')

    # print every available segment
    for segment in segments.get('items', []):
        print 'Segment Id         = %s' % segment.get('id')
        print 'Segment segmentId  = %s' % segment.get('segmentId')
        print 'Segment kind       = %s' % segment.get('kind')
        print 'Segment Type       = %s' % segment.get('type')
        print 'Segment Definition = %s' % segment.get('definition')
        print 'Segment Name       = %s' % segment.get('name')
        if segment.get('created'):
            print 'Created : %s' % segment.get('created'), '| Updated : %s' % segment.get('updated')
        print

if __name__ == '__main__':
    main(sys.argv)

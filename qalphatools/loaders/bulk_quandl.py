import json
import sys
import time
import os
import zipfile


def bulk_fetch(url, dest_file_ref):
    """
    Code adapted from http://www.sharadar.com/meta/bulk_fetch.py
    :param url: URL to get full Sharadar table from. You can specify different tables (SF1, SEP,...)
    Need to also specify Quandl API key
    url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s'% (table, api_key)
    optionally add parameters to the url to filter the data retrieved, as described in the associated table's
     documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started
    :param dest_file_ref: destination that you would like the retrieved data to be saved to
    """
    version = sys.version.split(' ')[0]
    if version < '3':
        import urllib2
        fn = urllib2.urlopen
    else:
        import urllib
        fn = urllib.request.urlopen

    valid = ['fresh', 'regenerating']
    status = ''
    while status not in valid:
        Dict = json.loads(fn(url).read().decode('utf-8'))
        last_refreshed_time = Dict['datatable_bulk_download']['datatable']['last_refreshed_time']
        status = Dict['datatable_bulk_download']['file']['status']
        link = Dict['datatable_bulk_download']['file']['link']
        print(status)
        if status not in valid:
            time.sleep(60)

    print('fetching from %s' % link)
    zipString = fn(link).read()
    f = open(dest_file_ref, 'wb')
    f.write(zipString)
    f.close()
    print('fetched')


def download_quandl(tables):
    """
    Download and unzip Sharadar tables from Quandl
    :param tables: List of strings, specifying tables to download
    """
    base_download = os.environ['QUANDL_BASE'] + 'data_downloads/'
    dest_file_ref = [base_download + t + '_download.csv.zip' for t in tables]
    base_url = os.environ['QUANDL_BASE_URL']

    for table, dest in zip(tables, dest_file_ref):

        url = base_url % (table, os.environ['QUANDL_API_KEY'])
        bulk_fetch(url, dest)

        with zipfile.ZipFile(dest, "r") as zip_ref:
            zip_ref.extractall(base_download)
            print("Unzipped: ", dest)

        os.remove(dest)
        print("Removed zip file: ", dest)


def get_newest_files(tables):
    """
    Find latest bulk file for each table and delete old files
    :param tables: List of strings
    :return: Dictionary with strings as key specifying a table, and the path of the latest file for each table
    """

    base_download = os.environ['QUANDL_BASE'] + 'data_downloads/'
    dest_file_ref = [base_download + t + '_download.csv.zip' for t in tables]

    newest_files = {}
    for table, dest in zip(tables, dest_file_ref):

        current_files = []
        for file in os.listdir(base_download):
            if file.startswith('SHARADAR_' + table):
                current_files.append(file)

        newest_files[table] = max([base_download + c for c in current_files], key=os.path.getctime)

        if len(current_files) > 1:
            for cf in current_files:
                if base_download + cf != newest_files[table]:
                    os.remove(base_download + cf)
                    print("Removed old file: ", base_download + cf)

    return newest_files


import logging
import MySQLdb
import dbutils
import urllib2
import zipfile
import os, errno

logging.basicConfig(filename='./log/detailed.log', level=logging.INFO)

def validate_date(loading_date):
    total_row_count=0

    try:
        mydb = MySQLdb.connect(host=dbutils.db_host,
            user=dbutils.db_user,
            passwd=dbutils.db_pass,
            db=dbutils.db_name)
        cursor = mydb.cursor()

        query="select count(*) as count from nse_bhavcopy where timestamp='{}';".format(loading_date.date())
        cursor.execute(query)

        row = cursor.fetchone()
        total_row_count = row[0]

        #close the connection to the database.
        mydb.commit()
        cursor.close()
    except:
        logging.error("Erro fetching data from database.")

    return total_row_count


def download_bhavcopy(loading_date):
    try:
        nse_url="https://www.nseindia.com/content/historical/EQUITIES/{}/{}/cm{}bhav.csv.zip".format(loading_date.strftime("%Y"),loading_date.strftime("%b").upper(),loading_date.strftime("%d%b%Y").upper())
        logging.info ("Attempting to download file: {}".format(nse_url))

        csv_name="tmp/cm{}bhav.csv".format(loading_date.strftime("%d%b%Y").upper())
        file_name=csv_name+".zip"

        req = urllib2.Request(nse_url, headers={'User-Agent' : "Magic Browser"})
        con = urllib2.urlopen(req)
        data = con.read()

        # Write data to file
        file_ = open(file_name, 'w')
        file_.write(data)
        file_.close()

    except Exception as e:
        logging.error(e)
        logging.info("No downloads available for this date.")
        return 1

    try:
        #unzip file
        zip_ref = zipfile.ZipFile(file_name, 'r')
        zip_ref.extractall("tmp")
        zip_ref.close()

        os.remove(file_name)
    except Exception as e:
        logging.error(e)
        return 1

    return 0

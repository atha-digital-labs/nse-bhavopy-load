#!/usr/bin/env python

import click
import csv
import datetime
import os, errno
import MySQLdb
import logging
import nseutils
import dbutils


@click.command(help="Run this script to load bhavcopy CSV from from NSE to local mySQL database. This files can be loaded for a give date or month or year.")

@click.option('--range-value', default=1, show_default=True, help='Number of days data needs to be downloaded. 1 for current day.')
@click.option('--sql-database', default='nse_dump', show_default=True, help='database name.')
@click.option('--sql-host', default='localhost', show_default=True, help='Host IP or DNS for mySQL database.')
@click.option('--sql-user', help='user name for database.')
@click.option('--sql-pass', help='database user\'s password')
@click.help_option('-h', '--help')

def execute_load_data(range_value, sql_database, sql_host, sql_user, sql_pass):
    if not range_value:
        raise click.UsageError("The range-value option must be provided")
    if not sql_database:
        raise click.UsageError("The sql-database option must be provided")
    if not sql_host:
        raise click.UsageError("The sql-host option must be provided")
    if not sql_user:
        raise click.UsageError("The sql-user option must be provided")
    if not sql_pass:
        raise click.UsageError("The sql-pass option must be provided")

    logging.basicConfig(filename='./log/detailed.log', level=logging.INFO)
    dbutils.init(sql_host,sql_user,sql_pass,sql_database)

    today=datetime.datetime.now()
    days=0

    while days < range_value:
        target_date=(today - datetime.timedelta(days))
        days=days+1

        logging.info("Processing data for date: {} ".format(target_date.strftime("%d%b%Y").upper()))

        if nseutils.validate_date(target_date) > 0:
            logging.info("data already available!!")
            continue

        if nseutils.download_bhavcopy(target_date) == 0:
            # Load csv data to MySQL database
            logging.info("loading csv to database!")

            try:
                mydb = MySQLdb.connect(host=sql_host,
                    user=sql_user,
                    passwd=sql_pass,
                    db=sql_database)
                cursor = mydb.cursor()

                csv_name="tmp/cm{}bhav.csv".format(target_date.strftime("%d%b%Y").upper())
                csv_data = csv.reader(file(csv_name))
                header=1
                for row in csv_data:
                    if(header==0):
                        query="INSERT INTO nse_bhavcopy(symbol,series,open,high,low, close,last,prevclose,tottrdqty,tottrdval,timestamp,totaltrades,isin ) VALUES ('{}', '{}',{}, {},{},{},{},{},{},{},'{}',{},'{}')".format(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],target_date.date(),row[11],row[12])
                        cursor.execute(query)
                    header=0
                #close the connection to the database.
                mydb.commit()
                cursor.close()
                logging.info("Load completed!!")

            except Exception as e:
                logging.error(e)

            try:
                os.remove(csv_name)
            except Exception as e:
                logging.error(e)



if __name__ == '__main__':
    execute_load_data()

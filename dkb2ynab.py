"""
Script to convert DKB transactions, contained in a CSV file into a
format that can be imported into YNAB. The script will connect to a
webdav server, download the CSV file, and then convert it to a format
that can be imported into YNAB. It then writes the converted file into
a different folder on the webdav server. It also analyzes the filename
and will only import a certain date range if the file name is in the
correct format.
"""

from datetime import datetime
from pathlib import Path
import logging
import csv
import re
import os
import time
from webdav3.client import Client
from webdav3.exceptions import WebDavException

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('dkb2ynab')

# date and time
today = datetime.now()
datestamp = today.strftime('%Y%m%d')

# collect environment variables
webdav_host = os.environ.get('WEBDAV_HOST')
webdav_user = os.environ.get('WEBDAV_USER')
webdav_password = os.environ.get('WEBDAV_PASSWORD')
csv_folder = os.environ.get('CSV_FOLDER')
ynab_folder = os.environ.get('YNAB_FOLDER')
interval = os.environ.get('INTERVAL')
workdir = os.environ.get('WORKDIR')

# webdav options
webdav_options = {
    'webdav_hostname': webdav_host,
    'webdav_login': webdav_user,
    'webdav_password': webdav_password
}


def convert_data(file, start_date=None, end_date=None) -> None:
    """
    Do the actual csv data conversation from the DKB to the YNAB format.
    """

    with open(file, 'r', encoding='utf-8-sig') as dkb_csv:
        # Field names for the DKB csv import file
        dkb_csv_fieldnames = [
            'Buchungsdatum',
            'Wertstellung',
            'Status',
            'Zahlungspflichtige*r',
            'Zahlungsempfänger*in',
            'Verwendungszweck',
            'Umsatztyp',
            'IBAN',
            'Betrag (€)',
            'Gläubiger-ID',
            'Mandatsreferenz',
            'Kundenreferenz'
        ]
        logger.debug('DKB CSV fieldnames: %s', dkb_csv_fieldnames)
        # Field names for the YNAB csv export file
        ynab_csv_fieldnames = [
            'Date',
            'Payee',
            'Memo',
            'Amount',
        ]
        logger.debug('YNAB CSV fieldnames: %s', ynab_csv_fieldnames)

        # Read the DKB CSV file
        dkb_reader = csv.DictReader(
            dkb_csv,
            fieldnames=dkb_csv_fieldnames,
            delimiter=';',
            quotechar='"'
        )
        # Let us iterate through the rows in the DKB CSV file
        for row in dkb_reader:
            logger.debug('Line number: %s', dkb_reader.line_num)
            logger.debug('Row: %s', row)
            # Check the first line of the CSV file and use the first
            # column as account name and the second column as IBAN.
            if dkb_reader.line_num == 1:
                account_name = row['Buchungsdatum']
                iban = row['Wertstellung']
                logger.info('Account name: %s', account_name)
                logger.info('IBAN: %s', iban)

                # Check if 'iban' matches a real valid IBAN using a
                # regular expression
                if not re.match(r'^DE\d{20}$', iban):
                    logger.error('Invalid IBAN: %s', iban)
                    raise ValueError(f'Invalid IBAN: {iban}')

                if start_date and end_date:
                    ynab_file_name = (
                        f'{workdir}/'
                        f'{datestamp}-{account_name}-'
                        f'{iban}_{start_date}_{end_date}.csv'
                    )
                else:
                    ynab_file_name = (
                        f'{workdir}/'
                        f'{datestamp}-{account_name}-{iban}.csv'
                    )
                logger.info('Writing local YNAB CSV file: %s', ynab_file_name)
                with open(
                    ynab_file_name, 'w', encoding='utf-8-sig'
                ) as ynab_file:
                    writer = csv.DictWriter(
                        ynab_file,
                        fieldnames=ynab_csv_fieldnames
                    )
                    writer.writeheader()

            # Real data begins after the first 5 lines.
            if dkb_reader.line_num > 5:
                # Convert the date format from 'DD.MM.YYYY'
                # to 'YYYY-MM-DD'
                date = datetime.strptime(
                    row['Buchungsdatum'], '%d.%m.%y'
                )
                date_str = date.strftime('%Y-%m-%d')

                # If we have a start and end date, we only add lines
                # for the desired date range.
                if start_date and end_date:
                    # Skip the row if the date is outside the
                    # specified range
                    if date < start_date or date > end_date:
                        continue

                # Convert the amount format from 'X.XXX,XX' to 'X.XX'
                amount = row['Betrag (€)'].replace(
                    '.', '').replace(',', '.')

                # Write the row to the YNAB CSV file
                ynab_row = {
                    'Date': date_str,
                    'Memo': row['Verwendungszweck'],
                    'Amount': amount
                }

                # If we have an inflow of money, we need to switch
                # payee and payer information
                if row['Umsatztyp'] == 'Eingang':
                    ynab_row['Payee'] = row['Zahlungspflichtige*r']
                else:
                    ynab_row['Payee'] = row['Zahlungsempfänger*in']

                # Skip the last line if it is the DKB summary that
                # provides information about the interested rates and
                # the total balance from before the first transaction
                if (
                    row['Zahlungspflichtige*r'] == 'DKB AG' and
                    'Kontostand/Rechnungsabschluss' in
                    row['Verwendungszweck']
                   ):
                    continue

                with open(
                    ynab_file_name, 'a', encoding='utf-8-sig'
                ) as ynab_file:
                    ynab_writer = csv.DictWriter(
                        ynab_file,
                        fieldnames=ynab_csv_fieldnames,
                        delimiter=','
                    )
                    ynab_writer.writerow(ynab_row)
                    logger.debug(
                        'Row written to local YNAB CSV file: %s',
                        ynab_row
                    )

        try:
            # Upload the converted file to the webdav server
            upload_webdav_file(
                webdav_options,
                ynab_file_name,
                ynab_folder
            )
        except Exception as e:
            logger.error("Failed to upload file %s (%s)",
                         ynab_file_name, e)

        try:
            # Delete the converted file after uploading
            Path(ynab_file_name).unlink()
            logger.info("Deleted local YNAB CSV file: %s", ynab_file_name)
        except Exception as e:
            logger.error("Failed to delete file %s (%s)",
                         ynab_file_name, e)


def delete_webdav_file(options, remote_file):
    """Delete remote file from WebDAV."""
    client = Client(options)
    try:
        client.clean(remote_file)
        logging.info("Successfully deleted file '%s' from WebDAV",
                     remote_file)
    except WebDavException as exception:
        logging.error("Could not delete file '%s'"
                      " from WebDAV (%s)",
                      remote_file, exception)


def download_webdav_files(options, remote_dir, local_dir):
    """
    This code defines a function to download files from a WebDAV
    server to a local directory. It connects to the WebDAV server,
    lists files in the specified directory, filters out hidden
    files, and then downloads each non-hidden file to the local
    directory. If the WebDAV directory does not exist, it logs an
    error message.
    """
    # Connect to WebDAV
    client = Client(options)
    try:
        client.check(remote_dir)
        logging.info("WebDAV directory '%s' exists."
                     " Retrieving contents...", remote_dir)
        # List files in the directory
        webdav_files = client.list(remote_dir)
        # Webdav always gives the current dir as first element.
        # We remove that first element from the list.
        del webdav_files[0]
        # We also do not want hidden files, so we remove them
        # as well from the output
        safe_files = [f for f in webdav_files if not f.startswith('.')]
        if safe_files:
            # Process each file
            for webdav_file in safe_files:
                remote_filename = f"{remote_dir}/{webdav_file}"
                local_filename = f"{local_dir}/{webdav_file}"
                try:
                    client.download_sync(remote_filename, local_filename)
                    logging.info("Successfully downloaded file '%s'"
                                 " from WebDAV directory '%s'",
                                 webdav_file, remote_dir)
                    # Disabled for debugging purposes
                    delete_webdav_file(options, remote_filename)
                except WebDavException as exception:
                    logging.error("Could not download file '%s'"
                                  " from WebDAV directory '%s': %s",
                                  webdav_file, remote_dir, exception)
        else:
            logging.info("No new files found in WebDAV directory '%s'",
                         remote_dir)
    except WebDavException as e:
        logging.error("WebDAV directory '%s' does not exist: %s",
                      remote_dir, e)


def upload_webdav_file(options, local_file, remote_dir):
    """
    Upload the converted CSV file to the WebDAV server.
    """
    remote_filename = Path(local_file).name
    remote_filepath = f"{remote_dir}/{remote_filename}"
    client = Client(options)
    try:
        client.check(remote_dir)
        client.upload_sync(
            remote_path=remote_filepath,
            local_path=local_file
        )
        logging.info("Successfully uploaded file '%s' to WebDAV"
                     " directory '%s'", local_file, remote_dir)
    except WebDavException as exception:
        logging.error("Could not upload file '%s' to WebDAV"
                      " directory '%s': %s", local_file, remote_dir,
                      exception)


def convert_file(file):
    """
    Convert DKB CSV bank account export to YNAB coompatible CSV format.
    """

    # Define pattern of filename that only contains date range in the
    # format of 'YYYYMMDD-YYYYMMDD.csv'
    pattern = r'\d{8}-\d{8}\.csv'

    # If the pattern matches, we need to extract the date rangtes and
    # treat the file differently. Only converting the transactions
    # within the date range
    if re.match(pattern, file.name):
        start_date_str = file.name.split('-')[0]
        end_date_str = file.name.split('-')[1].split('.')[0]
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        try:
            convert_data(file, start_date, end_date)
        except Exception as e:
            logger.error("Failed to convert file %s with particular "
                         "start and end range: %s",
                         file.name, e)
    else:
        try:
            convert_data(file)
        except Exception as e:
            logger.error("Failed to convert file '%s': %s",
                         file.name, e)

        # Deleting the file after successfully conversion. If the
        # conversion fails, the file will not be deleted as the
        # exeception hapepned before the deletion.
        logger.info("Deleting source CSV '%s' after successful conversion",
                    file)
        Path(file).unlink()


def main() -> None:
    """
    Main function to process files and sleep for the configured
    interval after each run.
    """

    # Create target directory if it doesn't exist
    if Path(workdir).exists():
        logging.debug("Directory '%s' already exists. "
                      "Nothing to do.", workdir)
    else:
        logging.warning("Directory '%s' does not exist. "
                        "Creating it...", workdir)
        try:
            Path(workdir).mkdir(parents=True)
            logging.info("Directory '%s' successfully created.",
                         workdir)
        except Exception as e:
            logging.error("Failed to create directory '%s': %s",
                          workdir, e)

    # Process files
    while True:
        # Download files from webdav into local work directory
        download_webdav_files(webdav_options, csv_folder, workdir)

        files = Path(workdir).glob('*.csv')
        if not files:
            logger.info(
                'No new files found in source directory: %s', csv_folder
            )
        else:
            for file in files:
                logger.info('Converting file: %s', file)
                convert_file(file)

        logger.info('Sleeping for %s seconds', interval)
        time.sleep(int(interval))


if __name__ == "__main__":
    main()

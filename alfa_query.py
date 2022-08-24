import json
import logging
import sys
import os
import argparse
import time
import pandas as pd
from io import StringIO
from Aries.storage import StorageFile
import requests
from concurrent.futures import ThreadPoolExecutor

from shared.utils import get_api_sleep

PAYLOAD_COUNT = 0

# Setting the format of the logs
FORMAT = '[%(process)d] %(name)s -- %(message)s'


def configure_logging():
    # Setting the format of the logs
    FORMAT = "[%(asctime)s] %(levelname)s: %(message)s"

    # Configuring the logging system to the lowest level
    logging.basicConfig(level=logging.DEBUG, format=FORMAT, stream=sys.stderr)

    # Defining the ANSI Escape characters
    BOLD = '\033[1m'
    DEBUG = '\033[92m'
    INFO = '\033[94m'
    WARNING = '\033[93m'
    ERROR = '\033[91m'
    END = '\033[0m'

    # Coloring the log levels
    if sys.stderr.isatty():
        logging.addLevelName(logging.ERROR, "%s%s%s%s%s" % (BOLD, ERROR, "ALFA_QUERY_ERROR", END, END))
        logging.addLevelName(logging.WARNING, "%s%s%s%s%s" % (BOLD, WARNING, "ALFA_QUERY_WARNING", END, END))
        logging.addLevelName(logging.INFO, "%s%s%s%s%s" % (BOLD, INFO, "ALFA_QUERY_INFO", END, END))
        logging.addLevelName(logging.DEBUG, "%s%s%s%s%s" % (BOLD, DEBUG, "ALFA_QUERY_DEBUG", END, END))
    else:
        logging.addLevelName(logging.ERROR, "ALFA_QUERY_ERROR")
        logging.addLevelName(logging.WARNING, "ALFA_QUERY_WARNING")
        logging.addLevelName(logging.INFO, "ALFA_QUERY_INFO")
        logging.addLevelName(logging.DEBUG, "ALFA_QUERY_DEBUG")

    # Setting the level of the logs
    level = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG][2]
    logging.getLogger().setLevel(level)


def configure_argparser(argparser_obj):
    argparser_obj.add_argument("-i",
                               action="store",
                               dest="input_file",
                               required=True,
                               help="Path to TSV file for processing.")

    argparser_obj.add_argument("-o",
                               action="store",
                               dest="output_file",
                               required=True,
                               help="Path to result TSV file.")

    argparser_obj.add_argument("--thread_count",
                               action="store",
                               type=int,
                               dest="thread_count",
                               required=False,
                               default=os.cpu_count() - 1,
                               help="Flag for separate output files or one combined output file.")


def increment_payload_count():
    global PAYLOAD_COUNT
    PAYLOAD_COUNT = PAYLOAD_COUNT + 1


def set_payload_count(num):
    global PAYLOAD_COUNT
    PAYLOAD_COUNT = num


def decrement_payload_count():
    global PAYLOAD_COUNT
    PAYLOAD_COUNT = PAYLOAD_COUNT + -1


def download(payload):
    try:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        body = json.dumps({"filter": payload["variants"]})
        processing = True
        attempt = 1
        while processing:
            try:
                response = requests.post("https://cs-api.davelab.org/alfa/filtered-results/", headers=headers, data=body)
                if not response or not hasattr(response, 'status_code' ) or response.status_code != 200 :
                    if attempt <= 7:
                        sleep = get_api_sleep(attempt)
                        time.sleep(sleep)
                        attempt += 1
                        continue
                    else:
                        logging.error(f"request failed, error: {e}")
                        decrement_payload_count()
                        return None
                logging.info(f"Request was successfully completed in {response.elapsed.total_seconds()} seconds [{PAYLOAD_COUNT} Remain]")
                decrement_payload_count()
                return response
            except Exception as e:
                if attempt <= 7:
                    sleep = get_api_sleep(attempt)
                    time.sleep(sleep)
                    attempt += 1
                    continue
                else:
                    logging.error(f"request failed, error: {e}")
                    decrement_payload_count()
                    return None
    except Exception as e:
        logging.error("There was an error with processing one of the payloads")
        logging.error(f"Error: {e}")
        decrement_payload_count()
        return None


def main():

    argparser = argparse.ArgumentParser(prog="AlfaQuery")
    configure_argparser(argparser)

    # Configure logging
    configure_logging()

    # Parse the arguments
    args = argparser.parse_args()

    THREAD_POOL = args.thread_count

    try:
        file_timer_start = time.time()
        if StorageFile(args.input_file).exists():
            # pull all the unique chrom-pos-ref-alt from the TSV file
            logging.info(f"Getting unique list of chrom-pos-ref-alt from the input file")
            with StorageFile.init(args.input_file) as f:
                content = StringIO(f.read())
                df = pd.read_csv(content, sep="\t")
                variant_cpra_list = df.CHROM_POS_REF_ALT.unique().tolist()
                variant_cpra_list = [s.replace("chr", "") for s in variant_cpra_list]

            logging.info(f"Total number of variants to process {len(variant_cpra_list)}")
            total_count = len(variant_cpra_list)
            alfa_data = []
            full_result = True
            payloads = []
            variants = []
            count = 0
            payload_index = 1
            for variant in variant_cpra_list:
                if count <= 500:
                    variants.append(variant)
                    count += 1
                else:
                    variants.append(variant)
                    payloads.append({"variants" : variants})
                    variants = []
                    count = 0
                    increment_payload_count()

            if len(variants) > 0:
                payloads.append({"variants" : variants})

            start_time = time.time()
            # query Crowdseq with the list of chrom-pos-ref-alt
            logging.info(f"Querying Crowdseq for population frequencies")
            with ThreadPoolExecutor(max_workers=THREAD_POOL) as executor:
                # wrap in a list() to wait for all requests to complete
                for response in list(executor.map(download, payloads)):
                    if response and hasattr(response, 'status_code') and response.status_code == 200:
                        alfa_data += response.json()
                    else:
                        full_result = False
            end_time = time.time()
            total_time = end_time - start_time
            logging.info(f"Request(s) were successfully completed in {total_time} seconds [{len(alfa_data)} results]")

            # concat the retrieved population frequencies to the result TSV file if found
            logging.info(f"Appending population frequencies to TSV")
            if alfa_data:
                temp = [{"CHROM_POS_REF_ALT": f"chr{x['chrom_pos_ref_alt']}",
                            "ALFA_EUR": x['EUR'],
                            "ALFA_AFO": x['AFO'],
                            "ALFA_EAS": x['EAS'],
                            "ALFA_AFA": x['AFA'],
                            "ALFA_LAC": x['LAC'],
                            "ALFA_LEN": x['LEN'],
                            "ALFA_OAS": x['OAS'],
                            "ALFA_SAS": x['SAS'],
                            "ALFA_OTR": x['OTR'],
                            "ALFA_AFR": x['AFR'],
                            "ALFA_ASN": x['ASN'],
                            "ALFA_TOT": x['TOT']} for x in alfa_data]
            alfa_df = pd.DataFrame(temp)
            df = df.merge(alfa_df, how='left')
            df.to_csv(args.output_file, sep='\t', index=False)

            logging.info(f"Processing complete!")
            t = time.time() - file_timer_start
            logging.info(f'File processing complete in {t:.3f} seconds.')
        else:
            logging.error("Input TSV file is not present! Alfa query failed.")
            raise "Script failed"

    except BaseException as e:
        # Report any errors that arise
        logging.error(f"Alfa Query failed!")
        err_msg = "Runtime error!"
        if e != "":
            err_msg += f"\nReceived the following error message:\n{e}"
        raise


if __name__ == "__main__":
    main()

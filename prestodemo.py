#!/usr/bin/env python3

import argparse
import configparser
import logging
import pandas
import prestodb
import sys

logging.basicConfig(stream=sys.stdout,
    format="%(asctime)s - %(levelname)s - %(message)s")

def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config",
        help="Configuration in INI file format (default: %(default)s)",
        default="prestodemo.ini", metavar="FILE")
    parser.add_argument("-l", "--log",
        help="Logging level: %(choices)s (default: %(default)s)",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO", metavar="LEVEL")
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log, None))
    logging.debug("Parsed command line arguments: %s" %args)

    return read_config(args.config)

def read_config(configfile):
    logging.debug("Reading configuration file: %s" %configfile)
    config = configparser.ConfigParser()
    config.read(configfile)
    return config

def main():
    config = parse_command_line()
    try:
        conn=prestodb.dbapi.connect(
            host=config["DB"]["host"],
            port=config["DB"]["port"],
            user=config["Creds"]["user"],
            catalog=config["DB"]["catalog"],
            schema=config["DB"]["schema"],
            http_scheme=config["DB"]["http_scheme"],
            auth=prestodb.auth.BasicAuthentication(
                config["Creds"]["user"], config["Creds"]["password"]),
        )

        cur = conn.cursor()
        logging.info("Executing query: %s" %config["DB"]["query"])
        cur.execute(config["DB"]["query"])
        results = cur.fetchall()
        cols = [col[0] for col in cur.description]
        data = pandas.DataFrame(results, columns=cols)

        logging.info("Results: %s Rows, %s Cols" %(data.shape[0], data.shape[1]))
          
    except Exception as e:
        logging.error("Error: %s" %e)
    finally:
        cur.cancel()
        conn.close()
    return

if __name__ == "__main__":
    main()


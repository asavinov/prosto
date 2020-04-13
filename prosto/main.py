import os
import sys
import argparse

from prosto.Prosto import *
from prosto.version import *

import logging
log = logging.getLogger('prosto')


def run(script_file):

    # TODO: Not implemented
    log.error("Script execution is not implemented.".format())

    return 0

def main(args = None):
    if not args: args = sys.argv[1:]

    programVersion = 'Version ' + VERSION
    programDescription = 'Prosto data processing toolkit' + programVersion

    parser = argparse.ArgumentParser(description=programDescription)
    parser.add_argument('-v', '--version', action='version', version=programVersion)

    parser.add_argument('-l', '--log', dest="loglevel", required=False, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help="Set the logging level (default INFO)")

    parser.add_argument('script_file', type=str, help='Script file name')

    arguments = parser.parse_args(args)

    # Configure logging
    logging.basicConfig(stream=sys.stderr, level=arguments.loglevel, format='%(asctime)s - %(name)s - %(levelname)s: %(message)s')

    # Environment
    log.info(programDescription)

    exitcode = 1
    try:
        exitcode = run(arguments.script_file)
    except Exception as e:
        log.error("Error executing script file {}.".format(arguments.script_file))
        log.exception(e)

    logging.shutdown()

    return exitcode

if __name__ == "__main__":

    exitcode = main(sys.argv[1:])

    if(not exitcode):
        exit()
    else:
        exit(exitcode)

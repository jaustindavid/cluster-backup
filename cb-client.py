#!/usr/bin/env python3

################################
#
#          M A I N
#
################################


import getopt, platform, sys, logging, os
import config
from client import Client


def getopts():
    options = {}
    options["hostname"] = platform.node()
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:c:v")
    except getopt.GetoptError as err:
        print(err)
        sys.exit(1)
    options["verbose"] = False
    for opt, arg in opts:
        if opt == "-c":
            options["configfile"] = arg
        elif opt == "-h":
            options["hostname"] = arg
        elif opt == "-v":
            options["verbose"] = True
        else:
            assert False, "Unhandled option"

    return options


def main(args):
    logger = logging.getLogger(__name__)
    logging.basicConfig(format='%(asctime)s [%(name)s] %(message)s',
                            level=logging.DEBUG)
    logger.setLevel(logging.INFO)

    cfg = config.Config.instance()
    options = getopts()
    if options['verbose']:
        logger.setLevel(logging.DEBUG)
    assert os.path.exists(options['configfile']), \
        f"Can't read {options['configfile']}"
    assert type(options["hostname"]) is str

    cfg.init(options['configfile'], "source", "backup")

    c = Client(options['hostname'])
    c.start()

    try:
        # wait for them to finish (if ever)
        c.join()
    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv)

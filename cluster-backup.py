#!/usr/bin/env python3

import sys, logging, os
import config, client, server

################################
#
#          M A I N
#
################################

import getopt, platform


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

    s = server.Server(options['hostname'])
    s.start()

    c = client.Client(options['hostname'])
    c.start()

    # wait for them to finish (if ever)
    s.join()
    c.join()


if __name__ == "__main__":
    main(sys.argv)

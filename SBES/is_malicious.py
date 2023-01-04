#!/usr/bin/env python
#  This script tells if a File, IP, Domain or URL may be malicious according to the data in OTX
import get_malicious
import hashlib


def is_malicious(otx, args):
    print(args)
    if args['ip']:
        alerts = get_malicious.ip(otx, args['ip'])
        if len(alerts) > 0:
            print('Identified as potentially malicious')
            print(str(alerts))
        else:
            print('Unknown or not identified as malicious')

    if args['host']:
        alerts = get_malicious.hostname(otx, args['host'])
        if len(alerts) > 0:
            print('Identified as potentially malicious')
            print(str(alerts))
        else:
            print('Unknown or not identified as malicious')

    if args['url']:
        alerts = get_malicious.url(otx, args['url'])
        if len(alerts) > 0:
            print('Identified as potentially malicious')
            print(str(alerts))
        else:
            print('Unknown or not identified as malicious')

    if args['hash']:
        alerts =  get_malicious.file(otx, args['hash'])
        if len(alerts) > 0:
            print('Identified as potentially malicious')
            print(str(alerts))
        else:
            print('Unknown or not identified as malicious')


    if args['file']:
        hash = hashlib.md5(open(args['file'], 'rb').read()).hexdigest()
        alerts =  get_malicious.file(otx, hash)
        if len(alerts) > 0:
            print('Identified as potentially malicious')
            print(str(alerts))
        else:
            print('Unknown or not identified as malicious')


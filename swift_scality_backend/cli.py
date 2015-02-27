# Copyright (c) 2015 Scality
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''Command-line utilities.'''

import argparse
import ConfigParser
import pkg_resources
import sys

from swift_scality_backend.policy_configuration import Configuration
from swift_scality_backend.policy_configuration import ConfigurationError
from swift_scality_backend.policy_configuration import \
    DEFAULT_CONFIGURATION_PATH
from swift_scality_backend.policy_configuration import StoragePolicy


def print_error(prefix, exn):
    '''Generic exception message printer.'''

    sys.stderr.write('%s: %s\n' % (prefix, exn.message))


def storage_policy_lint(args):
    '''Implementation of the 'storage-policy-lint' subcommand.'''

    try:
        Configuration.from_stream(args.config)
        return 0
    except ConfigurationError as exn:
        print_error('Configuration error', exn)
    except ConfigParser.Error as exn:
        print_error('Parsing error', exn)
    except Exception as exn:
        print_error('Error', exn)

    return 1


def storage_policy_query(args):
    '''Implementation of the 'storage-policy-query' subcommand.'''

    try:
        config = Configuration.from_stream(args.config)
        policy = config.get_policy(args.policy_index)
    except Exception as exn:
        print_error('Error', exn)
        return 1

    print 'Query'
    print '-----'
    print 'Policy index:', args.policy_index
    print 'Action:', args.action
    print 'Location hints:', args.locations
    print

    print 'Result'
    print '------'
    result = policy.lookup(args.action, args.locations)
    for (idx, endpoints) in enumerate(result):
        print '%d: %r' % (idx, list(sorted(str(e) for e in endpoints)))

    return 0


def main(args=None):
    '''Main entry point.'''

    parser = argparse.ArgumentParser(
        description='Scality Swift back-end utilities')
    subparsers = parser.add_subparsers(
        title='subcommands')

    try:
        package = pkg_resources.get_distribution('swift-scality-backend')
        version = '%s %s' % (package.project_name, package.version)
    except pkg_resources.DistributionNotFound:
        version = 'unknown'

    parser.add_argument('--version', action='version', version=version)

    # storage-policy-lint parser
    parser_sp_lint = subparsers.add_parser('storage-policy-lint')
    parser_sp_lint.set_defaults(func=storage_policy_lint)
    parser_sp_lint.add_argument(
        '-c', '--config',
        dest='config', action='store', type=argparse.FileType('r'),
        help='storage policy configuration to lint', metavar='FILE',
        default=DEFAULT_CONFIGURATION_PATH)

    # storage-policy-query parser
    parser_sp_query = subparsers.add_parser('storage-policy-query')
    parser_sp_query.set_defaults(func=storage_policy_query)
    parser_sp_query.add_argument(
        '-c', '--config',
        dest='config', action='store', type=argparse.FileType('r'),
        help='storage policy configuration to query', metavar='FILE',
        default=DEFAULT_CONFIGURATION_PATH)
    parser_sp_query.add_argument(
        'action',
        choices=[StoragePolicy.READ, StoragePolicy.WRITE],
        help='action to emulate', metavar='ACTION')
    parser_sp_query.add_argument(
        'policy_index',
        action='store', type=int,
        help='storage policy index', metavar='INDEX')
    parser_sp_query.add_argument(
        'locations',
        help='location hints', metavar='LOCATION',
        nargs='*')

    # Go
    args2 = parser.parse_args(args=args)
    rc = args2.func(args2)

    sys.exit(rc)


if __name__ == '__main__':
    main()

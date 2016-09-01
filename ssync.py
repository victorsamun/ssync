#!/usr/bin/env python3

import argparse
import contextlib
import os
import subprocess
import sys


STDIN = sys.stdin.fileno()
STDOUT = sys.stdout.fileno()


def error(msg):
    print('Error: ' + msg, file=sys.stderr)


def calc_md5(filename):
    try:
        rv = subprocess.check_output(['md5sum', '--binary', filename])
        return rv.split()[0].decode('utf-8')
    except subprocess.CalledProcessError as e:
        error('calculating md5 of {}: {}'.format(filename, e))
        return None


@contextlib.contextmanager
def input_(filename):
    with open(filename or STDIN, encoding='utf-8') as f:
        yield f


@contextlib.contextmanager
def output(filename):
    with open(filename or STDOUT, 'w', encoding='utf-8') as f:
        yield f


def main_analyze(args):
    # args: [--md5, output, path]
    with output(args.output) as f:
        with contextlib.redirect_stdout(f):
            if args.md5:
                print('# md5 filename')
            else:
                print('# filename')

            if not args.path.endswith('/'):
                args.path += '/'

            for (dirpath, dirnames, filenames) in os.walk(args.path):
                path = dirpath[len(args.path):]
                for fn in filenames:
                    if args.md5:
                        md5 = calc_md5(os.path.join(dirpath, fn))
                        if not md5:
                            continue

                        print(md5, end=' ')

                    print(os.path.join(path, fn))


def nonempty_reader(fileobj):
    for line in fileobj:
        line = line.strip()
        if not line:
            continue

        yield line


def col_reader(fileobj):
    columns = []

    for line in nonempty_reader(fileobj):
        if line.startswith('#'):
            columns = line.split()[1:]
            continue

        yield dict(zip(columns, line.split(maxsplit=len(columns))))


def main_compare(args):
    # [origin, remote, diff]
    with open(args.origin, encoding='utf-8') as origin:
        origin_files = {}

        for line in col_reader(origin):
            origin_files[line['filename']] = line.get('md5', None)

    with open(args.remote, encoding='utf-8') as remote:
        modified_files = set()
        remote_files = set()

        for line in col_reader(remote):
            (filename, md5) = (line['filename'], line.get('md5', None))

            if filename not in origin_files:
                remote_files.add(filename)
                continue

            if origin_files[filename] != md5:
                modified_files.add(filename)

            del origin_files[filename]

    origin_files = set(origin_files)

    with output(args.diff) as f:
        with contextlib.redirect_stdout(f):
            for fn in origin_files:
                print('O ' + fn)
            if origin_files:
                print()

            for fn in remote_files:
                print('R ' + fn)
            if remote_files:
                print()

            for fn in modified_files:
                print('M ' + fn)


def copyfile(src, dst):
    print('copy "{}" "{}"'.format(src, dst))


def removefile(filename):
    print('remove "{}"'.format(filename))
    #return os.remove(filename)


def main_sync(args):
    # [origin, remote, diff, origin_remove, remote_copy, modified_download]
    def origin(fn): return os.path.join(args.origin, fn)
    def remote(fn): return os.path.join(args.remote, fn)

    with input_(args.diff) as diff:
        for line in nonempty_reader(diff):
            (status, filename) = line.split(' ', 1)
            if status == 'O':
                if args.origin_remove:
                    removefile(origin(filename))
                else:
                    copyfile(origin(filename), remote(filename))
            elif status == 'R':
                if args.remote_copy:
                    copyfile(remote(filename), origin(filename))
                else:
                    removefile(remote(filename))
            elif status == 'M':
                if args.modified_download:
                    copyfile(remote(filename), origin(filename))
                else:
                    copyfile(origin(filename), remote(filename))


def main_error(args):
    error('command must be specified')
    return 1


COMMANDS = {
    'analyze': main_analyze,
    'compare': main_compare,
    'sync': main_sync,
    None: main_error
}


def parse_args(args):
    parser = argparse.ArgumentParser(
        description='Simple file synchronizer',
        prefix_chars='-/',
        epilog='Author: Samun Victor <victor.samun@gmail.com>'
    )

    subparser = parser.add_subparsers(
        title='command',
        description='synchronization command',
        dest='cmd'
    )

    cmd_analyze = subparser.add_parser('analyze', description='analyze folder')
    cmd_analyze.add_argument('--md5', action='store_true',
                             help='calculate md5 sum')
    cmd_analyze.add_argument('path', type=str, metavar='DIR',
                             help='folder to analyze')
    cmd_analyze.add_argument('output', type=str, metavar='OUTPUT',
                             nargs='?', default=None,
                             help='output file (`stdout` default)')

    cmd_compare = subparser.add_parser(
        'compare', description="compare two folders on analysis' result")
    cmd_compare.add_argument('origin', type=str, metavar='ORIGIN',
                             help='`origin` folder analysis')
    cmd_compare.add_argument('remote', type=str, metavar='REMOTE',
                             help='`remote` folder analysis')
    cmd_compare.add_argument('diff', type=str, metavar='DIFF',
                             nargs='?', default=None,
                             help='output file (`stdout` default)')

    cmd_sync = subparser.add_parser(
        'sync', description='make syncronization based on diff')
    cmd_sync.add_argument('--origin-remove', action='store_true',
                          help="remove file on `origin` if it doesn't "
                               "exists on `remote` (copy by default)")
    cmd_sync.add_argument('--remote-copy', action='store_true',
                          help="copy file to `origin` if it doesn't "
                               "exists on `origin` (remove by default)")
    cmd_sync.add_argument('--modified-download', action='store_true',
                          help="copy modifications from remote to origin "
                               "(`origin to remote` by default)")
    cmd_sync.add_argument('origin', type=str, metavar='ORIGIN',
                          help='`origin` folder')
    cmd_sync.add_argument('remote', type=str, metavar='REMOTE',
                          help='`remote` folder')
    cmd_sync.add_argument('diff', type=str, metavar='DIFF',
                          nargs='?', default=None,
                          help='input file (`stdin` default)')

    return parser.parse_args()


def main(args):
    params = parse_args(args)
    return COMMANDS[params.cmd](params)


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))

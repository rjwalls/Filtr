import argparse
import atexit
import os
import pydablooms
import tempfile
import shutil
import sys


CAPACITY = 1000000
ERROR_RATE = 0.001
BLOCK_SIZE = 1024


def finish():
    try:
        sys.stdout.close()
    except:
        pass
    try:
        sys.stderr.close()
    except:
        pass

atexit.register(finish)

def __get_tmp_bloom():
    """
    """
    tmpdir = tempfile.mkdtemp(prefix='dec0de-bloom')
    newpath = os.path.join(tmpdir, 'bloom_tmp.bin')
    bloom = pydablooms.Dablooms(capacity=CAPACITY, error_rate=ERROR_RATE, filepath=newpath)

    return bloom, tmpdir


def __filter_with_add(bloom, infile, outfile, offsetfile):
    """

    """
    block = infile.read(BLOCK_SIZE)

    while block != '':
        if not bloom.check(block):
            if offsetfile:
                line = "%d,%d%s" % (infile.tell(), outfile.tell(), os.linesep)
                offsetfile.write(line)
                
            outfile.write(block)
            bloom.add(block)


        block = infile.read(BLOCK_SIZE)


def __filter_no_add(bloom, bloom_self, infile, outfile, offsetfile):
    """

    """

    block = infile.read(BLOCK_SIZE)

    while block != '':
        if bloom_self.check(block) or bloom.check(block):
            pass
        else:
            if offsetfile:
                line = "%d,%d%s" % (infile.tell(), outfile.tell(), os.linesep)
                offsetfile.write(line)

            outfile.write(block)
            bloom_self.add(block)

        block = infile.read(BLOCK_SIZE)


def __remove_from_bloom(bloom, infile):
    """

    """
    block = infile.read(BLOCK_SIZE)

    while block != '':
        if bloom.check(block):
            bloom.delete(block)

        block = infile.read(BLOCK_SIZE)


def main():
    """
    By default will read from stdin and write to stdout using a temporary bloom file.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('infile', nargs='?', type=argparse.FileType('rb'), default=sys.stdin,
                        help="Defaults to stdin")
    parser.add_argument('outfile', nargs='?', type=argparse.FileType('wb'), default=sys.stdout,
                        help="Defaults to stdout")

    parser.add_argument('--offsetfile', type=argparse.FileType('w'), default=None,
                        help="Defaults to None, requires a named outfile")
    parser.add_argument('-b', '--bloom', dest='bloom', help='The path to the bloom filter to check against')

    #Add and delete are mutually exclusive
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-a', '--add', action='store_true', default=False,
                        help='add the phone to the bloom file.')
    group.add_argument('-d', '--delete', action='store_true', default=False,
                        help='remove the phone from the bloom file.')

    args = parser.parse_args()

    bloom_self, tmpdir = __get_tmp_bloom()

    try:
        if args.offsetfile:
            args.outfile.tell()
            args.offsetfile.write('#Input:%s; ' % args.infile.name)
            args.offsetfile.write('Output:%s%s' % (args.outfile.name, os.linesep))

            if args.bloom:
                args.offsetfile.write('#Bloom:%s%s' % (args.bloom, os.linesep))

    except IOError:
        #File.tell() does not work with pipes.
        sys.stderr.write('Warning: Cannot write offset file when the output file is a pipe.' + os.linesep)
        args.offsetfile = None

    if args.bloom and os.path.isfile(os.path.abspath(args.bloom)):
        bloom_abspath = os.path.abspath(args.bloom)
        #sys.stderr.write('Using bloom: %s' % bloom_abspath + os.linesep)
        bloom = pydablooms.load_dabloom(capacity=CAPACITY, error_rate=ERROR_RATE, filepath=bloom_abspath)
    elif args.bloom and args.add:
        bloom_abspath = os.path.abspath(args.bloom)
        bloom = pydablooms.Dablooms(capacity=CAPACITY, error_rate=ERROR_RATE, filepath=bloom_abspath)
        sys.stderr.write('Created bloom at %s' % bloom_abspath + os.linesep)
    else:
        if args.bloom:
            sys.stderr.write("Bloom file does not exist and we cannot create new bloom without the --add flag" + os.linesep)
        elif args.add:
            sys.stderr.write("Add option ignored without bloom file specified" + os.linesep)

        bloom = bloom_self
        sys.stderr.write('Created tmp bloom at %s' % tmpdir + os.linesep)

    if args.delete:
        __remove_from_bloom(bloom, args.infile)
    elif args.add:
        __filter_with_add(bloom, args.infile, args.outfile, args.offsetfile)
    else:
        __filter_no_add(bloom, bloom_self, args.infile, args.outfile, args.offsetfile)

    if os.path.exists(tmpdir) and os.getcwd() != tmpdir:
        args.outfile.close()
        del bloom
        shutil.rmtree(tmpdir)

    args.infile.close()
    args.outfile.close()


if __name__ == '__main__':
    main()
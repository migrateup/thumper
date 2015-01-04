#!/usr/bin/env python3

import os
import argparse
import sys
import multiprocessing
from PIL import Image

def get_args():
    parser = argparse.ArgumentParser(
        description='Thumper - fast mass image thumbnail generator',
        epilog='''You must specify exactly one of the --size, --height or --width options.

Specify the thumbnail size in one of two ways. Use --size if you
intend a square thumbnail. Thumper will automatically fill the
thumbnail within a square of that many pixels, using it as height if
it's tall and skinny, or width if it is short and squat.

Alternatively, use both the --width and --height options. You must use
both of them; the image will be resized to fill this rectangle.

''',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        )
    parser.add_argument('src_dir',
                        help='Directory containing source images')
    parser.add_argument('dest_dir',
                        help='Directory to create and write thumbnails in')
    parser.add_argument('--size', default=None, type=int,
                        help='Thumbnail size (max dimension, height or width) in pixels')
    parser.add_argument('--width', default=None, type=int,
                        help='Max thumbnail width in pixels')
    parser.add_argument('--height', default=None, type=int,
                        help='Max thumbnail height in pixels')
    parser.add_argument('--processes', default=None, type=int,
                        help='Number of worker processes. Default is # of cores/cpus on the system')
    args = parser.parse_args()

    # check for correct arg combos
    if args.size is None:
        if args.width is None or args.height is None:
            parser.error('You must specify the thumbnail size - either via --size, or via --width AND --height.')
    else:
        if not (args.width is None and args.height is None):
            parser.error('If you use --size, you can\'t use --width or --height.')

    # check for sensible values
    if args.size is not None and args.size <= 0:
        parser.error('Thumbnail size must be a positive number.')
    if args.width is not None and args.width <= 0:
        parser.error('Thumbnail width must be a positive number.')
    if args.height is not None and args.height <= 0:
        parser.error('Thumbnail height must be a positive number.')

    return args

def find_dest_path(src_dir, dest_dir, src_path):
    full_src_path = os.path.realpath(src_path)
    full_src_dir = os.path.realpath(src_dir)
    rel_src_path = full_src_path[len(full_src_dir):]
    return dest_dir + rel_src_path

def create_thumbnail(src_path, dest_path, thumbnail_width, thumbnail_height):
    try:
        image = Image.open(src_path)
        image.thumbnail((thumbnail_width, thumbnail_height))
    except OSError as err:
        return {
            'success'  : False,
            'pid'      : os.getpid(),
            'src_path' : src_path,
            'error'    : str(err),
        }
    except Image.DecompressionBombWarning as warn:
        return {
            'success'  : False,
            'pid'      : os.getpid(),
            'src_path' : src_path,
            'error'    : str(warn),
        }
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    image.save(dest_path)
    return {
        'success'  : True,
        'pid'      : os.getpid(),
        'src_path' : src_path,
        }

def log_creation_done(results):
    for result in results:
        print(result)

if __name__ == '__main__':
    args = get_args()
    if os.path.isdir(args.dest_dir):
        sys.stderr.write('Dest directory "{}" already exists.\n'.format(args.dest_dir))
        sys.exit(1)
    if args.size:
        thumbnail_width = args.size
        thumbnail_height = args.size
    else:
        thumbnail_width = args.width
        thumbnail_height = args.height

    def gen_child_args():
        for (dirpath, dirnames, filenames) in os.walk(args.src_dir):
            for filename in filenames:
                src_path = os.path.join(dirpath, filename)
                dest_path = find_dest_path(args.src_dir, args.dest_dir, src_path)
                yield (src_path, dest_path, thumbnail_width, thumbnail_height)

    pool = multiprocessing.Pool(args.processes)
    pool.starmap_async(
        create_thumbnail,
        gen_child_args(),
        callback=log_creation_done,
        )
    pool.close()
    pool.join()

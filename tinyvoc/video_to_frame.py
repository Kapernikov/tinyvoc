#!/usr/bin/python3
import sys
import os
import argparse
import pathlib

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dataset preparation")
    parser.add_argument("--source", type=pathlib.Path, required=True, help="input movie")
    parser.add_argument("--destination", type=pathlib.Path, required=True, help="output directory (will be deleted)")
    parser.add_argument("--prefix", type=str, required=True, help="prefix for images (instead of 'frame')", default='frame')
    return parser.parse_args()



def main():
    args = get_args()
    pth = os.path.abspath(args.source)
    opth = os.path.abspath(args.destination)
    os.system("rm {opth}/*".format(opth=opth))
    os.system("mkdir -p {opth}".format(opth=opth))
    os.chdir(opth)
    os.system("ffmpeg -i {a} -vsync 0 tmpframe_%06d.PNG".format(a=pth))
    imgs = [x for x in os.listdir(".") if x.endswith("PNG")]
    framenrs = [int(x.split("_")[1].split(".")[0]) for x in imgs]
    pfx = args.prefix
    if not 0 in framenrs:
        for nr,i in zip(framenrs,imgs):
            new_framenr = pfx + "_%06d.PNG" % (nr-1)
            os.rename(i, new_framenr)

if __name__ == '__main__':
    main()





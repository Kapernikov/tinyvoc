#!/usr/bin/python3
import sys
import os
import argparse
import pathlib
from .pvocutils import DataLineage, LineageSource, SingleFileLineageSource, filter_args_for_datalineage

def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dataset preparation")
    parser.add_argument("--source", type=pathlib.Path, required=True, help="input zipfile")
    parser.add_argument("--destination", type=pathlib.Path, required=True, help="output directory (will be deleted)")
    return parser.parse_args()



def main():
    args = get_args()
    pth = os.path.abspath(args.source)
    opth = os.path.abspath(args.destination)
    lineage = DataLineage()
    lineage.add_source(SingleFileLineageSource(pth))
    for k,v in filter_args_for_datalineage(vars(args)).items():
        lineage.add_param(k,v)
    lineage_fn = str(args.destination.absolute())
    if lineage_fn.endswith("/"):
        lineage_fn = lineage_fn[:-1]
    lineage_fn += "_frames_lineage.yaml"
    if os.path.isfile(lineage_fn) and os.path.isdir(opth):
        old_lineage = DataLineage(lineage_fn)
        if lineage.is_uptodate_with(old_lineage):
            print("nothing to do, already done")
            sys.exit(0)
    os.system("rm {opth}/*".format(opth=opth))
    os.system("mkdir -p {opth}".format(opth=opth))
    os.chdir(opth)
    os.system("unzip {a}".format(a=pth))
    lineage.dump_yaml(lineage_fn)

if __name__ == '__main__':
    main()





import argparse
import os,sys
from tinyvoc.pvocutils import *
import yaml
import pathlib
import xml.etree.ElementTree as ET
import zipfile
import logging
import json

from typing import List, Dict


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a dataset based on multiple source datasets, avoiding filename conflicts")
    parser.add_argument("--source", type=pathlib.Path, help="path for input", required=True, action='append')
    parser.add_argument("--destination", type=pathlib.Path, required=True, help="path for output")
    return parser.parse_args()



def main():
    logging.basicConfig(level=logging.INFO)
    args = get_args()
    sources = []
    for src in args.source:
        if str(src).lower().endswith(".zip"):
            bdir = os.path.split(src)[0]
            sources.append(AnnotationZip(src, bdir))
        else:
            sources.append(AnnotationDirectory(src))
    os.makedirs(args.destination, exist_ok=True)
    writer = DirAnnotationWriter(args.destination)
    lineage = DataLineage()
    for s in sources:
        lineage.add_source(s.as_lineage_source())
    for k,v in vars(args).items():
        if type(v) in [int, str, bool, pathlib.Path]:
            lineage.add_param(k,v)
    if writer.check_lineage_okay(lineage):
        print("dataset already okay, doing nothing")
        sys.exit(0)
    for s in sources:
        for a in s.generate_annotations():
            assert isinstance(a, PascalVocAnnotation)
            writer.add_annotation(a,ImageTreatmentSetting.SYMLINK_IMAGE_RENAME)
    writer.write_dataset_meta()
    writer.write_lineage(lineage)

    print("SUMMARY")
    print("=======")
    for k in writer.metrics.keys():
        print("{k}: {v}".format(k=k, v=writer.metrics[k]))



if __name__ == "__main__":
    main()
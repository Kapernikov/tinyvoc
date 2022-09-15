import argparse
from importlib.resources import path
import os,sys
from .pvocutils import *
import yaml
import pathlib
import xml.etree.ElementTree as ET
import zipfile
import logging
import json


from typing import List, Dict

def process_annotation(annot: PascalVocAnnotation, valid_labels, concat_type=False, prefix=None) -> PascalVocAnnotation:
    if concat_type:
        for o in annot.objects:
            if 'Type' in o.attributes:
                o.name = o.name + o.attributes['Type']
    if len(valid_labels) > 0:
        annot.objects = [x for x in annot.objects if x.name in valid_labels]
        
    if prefix:
        annot.filename = annot.filename.replace("frame", prefix)
        annot.id = annot.id.replace("frame",prefix)
    annot.folder = ""
    return annot



def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dataset preparation")
    parser.add_argument("--parameters", help="path to parameters.yaml file", type=argparse.FileType("r", encoding="utf8"), required=False)
    parser.add_argument("--root", type=pathlib.Path, required=True, help="root folder for constructing relative paths")
    parser.add_argument("--source", type=argparse.FileType("rb"), help="zipfile with pascalvoc 1.1 annotations (input)", required=True)
    parser.add_argument("--destination", type=pathlib.Path, required=True, help="path for output")
    parser.add_argument("--label", type=str, required=False, help="allowed label (repeat this option to have multiple allowed labels)", action="append")
    parser.add_argument("--prefix", type=str, required=True, help="prefix for images (instead of 'frame')", default='frame')
    parser.add_argument("--export-imagesets", help="also write an ImageSets folder (default)", default=True)
    parser.add_argument("--metrics", type=pathlib.Path, help="metrics file to write")
    parser.add_argument("--concat-type", action="store_true", help="concat type attribute to label")
    parser.add_argument("--no-rewrite",  action="store_true", help="disable filename sanitizing and rewriting: keep original filenames and keep annotations for missing files")

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)
    args = get_args()
    if args.parameters:
        params = yaml.load(args.parameters, Loader=yaml.FullLoader)
        labels = params["annotations"]["valid-labels"]
        typeconcat = params["annotations"]["concat-type-attribute"]
    else:
        labels = []
        typeconcat = args.concat_type
    if args.label:
        for l in args.label:
            labels.append(l)
    os.makedirs(args.destination, exist_ok=True)
    os.system("rm {s}/*.xml".format(s=args.destination))
    writer = DirAnnotationWriter(args.root, args.destination)
    gen = AnnotationZip(args.source)
    l = DataLineage()
    for k,v in vars(args).items():
        if type(v) in [int, str, bool, pathlib.Path]:
            l.add_param(k,v)
    treat_way = ImageTreatmentSetting.REWRITE_RELPATH
    if args.no_rewrite:
        treat_way = ImageTreatmentSetting.KEEP_PATH
    l.add_source(gen.as_lineage_source())
    if writer.check_lineage_okay(l):
        print("dataset is up to date, doing nothing")
        sys.exit(0)
    for annot in gen.generate_annotations():
        processed = process_annotation(annot, labels, concat_type=typeconcat, prefix=args.prefix)
        if len(processed.objects) > 0:
            writer.add_annotation(processed, treat_way)

    if args.metrics:
        json.dump(writer.metrics, open(args.metrics,"w"))

    writer.write_lineage(l)
    if args.export_imagesets:
        writer.write_dataset_meta()


if __name__ == "__main__":
    main()

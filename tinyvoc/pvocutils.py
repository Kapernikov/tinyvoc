from __future__ import annotations
from genericpath import isfile
from importlib.util import source_hash
from posixpath import islink
import xml.etree.ElementTree as ET
from typing import List, Dict, IO, Optional, Tuple, Union, Generator
import zipfile
import os, shutil
from enum import Enum
import logging, pathlib
from .hashutil import hash_from_Str, hash_from_file
import yaml

def SingleFileLineageSource(fn):
    hash = hash_from_file(fn)
    l = LineageSource()
    l.image_path = fn
    l.source_hash = hash
    return l

class LineageSource(object):
    """
        A LineageSource is a reference to a source dataset that was used to create this dataset. It might recursively refer other lineage sources
    """
    def __init__(self):
        self.annotation_path = ''
        self.image_path = ''
        self.root_dir = ''
        self.source_hash = ''
        self.sources = []
        pass
    
    def _load_from_dict(self, dct):
        self.annotation_path = dct['annotation_path']
        self.image_path = dct['image_path']
        if "sourcehash" in dct:
            self.source_hash = dct["sourcehash"]
        self.root_dir = dct['root_dir']
        def create_from_dct(d):
            l = LineageSource()
            l._load_from_dict(d)
            return l
        self.sources = [create_from_dct(x) for x in dct['sources']]
    
    def _to_dict(self):
        d = {}
        d['annotation_path'] = self.annotation_path
        d['image_path'] = self.image_path
        d['root_dir'] = self.root_dir
        d["sourcehash"] = self.source_hash
        d['sources'] = [ x._to_dict() for x in self.sources ]
        return d

class DataLineage(object):
    """
        A DataLineage object holds information about how this dataset was created (which source datasets were used)
    """
    def __init__(self, src: Optional[Union[str, IO]] = None) -> None:
        self.data = { 'sources' : [], 'params': {} }
        if isinstance(src, str):
            src = open(src, 'r')
        if src is None:
            pass
        else:
            self.data = yaml.load(src, yaml.Loader)
        if not "params" in self.data:
            self.data["params"] = {}
    

    def add_param(self, key: str, val: Union[str, int, bool]):
        self.data["params"][key] = val

    @property
    def sources(self) -> List[LineageSource]:
        d = self.data
        def create_src(d) -> LineageSource:
            l = LineageSource()
            l._load_from_dict(d)
            return l
        return [create_src(x) for x in d['sources']]
    
    def compute_hash(self):
        buf = ""
        hasempty = False
        for s in self.sources:
            buf += s.source_hash
            if s.source_hash == "":
                hasempty = True
        
        params = list(self.data["params"].items())
        params = [f"{a[0]}:{a[1]}" for a in params]
        params.sort()
        params = ",".join(params)
        self.data["dataset_hash"] = hash_from_Str(buf+params)
        self.data["has_sources_without_hash"] = hasempty

    def get_hash(self) -> Tuple(str, bool):
        if not "dataset_hash" in self.data or self.data["dataset_hash"] == "":
            self.compute_hash()
        return (self.data["dataset_hash"], not self.data["has_sources_without_hash"])
    
    def is_uptodate_with(self, other: "DataLineage"):
        h, ok = self.get_hash()
        other_h, other_ok = other.get_hash()
        if (not ok) or (not other_ok):
            logging.info(f"cannot check for up to dateness as hashes are missing {self}")
            return False
        if h == other_h:
            return True
        return False

    def dump_yaml(self, path: str):
        self.compute_hash()
        with open(path,'w') as f:
            f.write(yaml.dump(self.data, Dumper=yaml.Dumper))

    @sources.setter
    def sources(self, s: List[LineageSource]):
        self.data['sources'] = [x._to_dict() for x in s]
    
    def as_source(self) -> LineageSource:
        l = LineageSource()
        l.sources = self.sources
        buf = ""
        hasempty = False
        for s in self.sources:
            buf += s.source_hash
            if s.source_hash == "":
                hasempty = True
        l.source_hash = hash_from_Str(buf)
        return l

    def add_source(self, src):
        s = self.sources
        s.append(src)
        self.sources = s


class BoundingBox(object):
    def __init__(self) -> None:
        self.xmin = None
        self.xmax = None
        self.ymin = None
        self.ymax = None
    
    def overlaps(self, other: BoundingBox) -> bool:
        if (self.xmax < other.xmin) or (self.xmin > other.xmax):
            return False
        if (self.ymax < other.ymin) or (self.ymin > other.ymax):
            return False
        return True
    


class PascalVocObject(object):
    def __init__(self,el) -> None:
        self.el = el
    
    @property
    def name(self) -> str:
        return self.el.find("name").text
    
    @property
    def occluded(self) -> bool:
        if not self.el.find("occluded"):
            return False
        return int(self.el.find("occluded").text) == 1

    @occluded.setter
    def occluded(self, o:bool) -> None:
        if not self.el.find("occluded"):
            self.el.append(ET.Element("occluded"))
        self.el.find("occluded").text = "1" if o else "0"

    @name.setter
    def name(self, n: str):
        self.el.find("name").text = n
    
    @property
    def boundingbox(self) -> BoundingBox:
        bndbox = self.el.find("bndbox")
        if bndbox:
            b = BoundingBox()
            b.xmin = float(bndbox.find("xmin").text)
            b.xmax = float(bndbox.find("xmax").text)
            b.ymin = float(bndbox.find("ymin").text)
            b.ymax = float(bndbox.find("ymax").text)
            return b
        return None

    @property
    def attributes(self) -> Dict[str, str]:
        d = {}
        for a in self.el.find("attributes").findall("attribute"):
            a_key = a.find("name").text
            a_val = a.find("value").text
            d[a_key] = a_val
        return d


class PascalVocAnnotation(object):
    def __init__(self, src, annotation_filename=None, root_directory=None) -> None:
        self.annotation_id = None
        self.annotation_fn = annotation_filename
        self.root_directory = root_directory

        if isinstance(src, ET.ElementTree):
            self.tree = src
            return 
        if isinstance(src, str):
            src = open(src,"r")
        self.tree = ET.parse(src)
    
    @property
    def id(self) -> str:
        if self.annotation_id is not None:
            return self.annotation_id
        if self.annotation_fn is not None:
            fn = self.annotation_fn
        else:
            fn = self.filename
        base_fn = os.path.split(fn)[1]
        without_ext = os.path.splitext(base_fn)[0]
        return without_ext

    @id.setter
    def id(self, id: str):
        self.annotation_id = id

    @property
    def objects(self) -> List[PascalVocObject]:
        return [PascalVocObject(x) for x in self.tree.getroot().findall("object")]
    
    @objects.setter
    def objects(self, objs: List[PascalVocObject]):
        cur = self.tree.getroot().findall("object")
        for o in objs:
            if not o.el in cur:
                raise Exception("adding objects not yet implemented")
        objs = [x.el for x in objs]
        for o in cur:
            if not o in objs:
                self.tree.getroot().remove(o)

    @property
    def filename(self) -> str:
        return self.tree.getroot().find("filename").text
    
    @property
    def annotation_filename(self) -> Optional[str]:
        return self.annotation_fn

    @filename.setter
    def filename(self, fn: str):
        self.tree.getroot().find("filename").text = fn

    @property
    def folder(self) -> str:
        return self.tree.getroot().find("folder").text
    
    @folder.setter
    def folder(self, fn: str):
        self.tree.getroot().find("folder").text = fn
        
    def write(self, fileobj: Union[str,IO]):
        self.tree.write(fileobj)


class ImageTreatmentSetting(Enum):
    KEEP_PATH=1
    REWRITE_RELPATH=2
    REWRITE_ABSPATH=3
    COPY_IMAGE=4
    SYMLINK_IMAGE=5
    COPY_IMAGE_RENAME=6
    SYMLINK_IMAGE_RENAME=7

class DirAnnotationWriter(object):
    def __init__(self, root_dir: str, annotation_output_dir: Optional[str] = None) -> None:
        self.root_dir = root_dir
        if annotation_output_dir is None:
            annotation_output_dir = os.path.join(self.root_dir, "Annotations")
        self.annotation_output_dir = annotation_output_dir
        self.image_dir = os.path.join(self.root_dir, "JPEGImages")
        os.makedirs(self.root_dir, exist_ok=True)
        os.makedirs(self.annotation_output_dir, exist_ok=True)
        self.rename_counter = 0
        self.metrics = {}
        self.ids = []
        self.extra_search_path = []

    def _log_object(self, label):
        if not label in self.metrics:
            self.metrics[label] = 0
        self.metrics[label] = self.metrics[label] + 1
        

    def add_annotation(self,annotation: PascalVocAnnotation, treat_image: ImageTreatmentSetting) -> None:
        fn = annotation.filename
        src_root_dir = annotation.root_directory
        if src_root_dir is None:
            src_root_dir = self.root_dir
        img_path = ''
        rel_fn = os.path.basename(fn)
        search_for_image = []
        for sp in self.extra_search_path:
            search_for_image.append(os.path.join(sp, fn)),
            search_for_image.append(os.path.join(sp, rel_fn)),

        search_for_image.extend([
            fn,
            os.path.join(src_root_dir, fn),
            os.path.join(src_root_dir, "JPEGImages", fn),
            rel_fn,
            os.path.join(src_root_dir, rel_fn),
            os.path.join(src_root_dir, "JPEGImages", rel_fn),
        ])

        for candidate in search_for_image:
            if os.path.isfile(candidate) and os.path.exists(candidate):
                img_path = candidate
        if ((img_path == '') and (treat_image != ImageTreatmentSetting.KEEP_PATH)):
            logging.warning(f"need to rewrite path but image does not exist {annotation.id} name={fn}, removing annotation")
            return None
        elif img_path == '':
            print(f"path {fn} {img_path} {os.path.abspath(img_path)}")
            img_path = fn
        fn = img_path

        self.rename_counter += 1
        if treat_image == ImageTreatmentSetting.REWRITE_ABSPATH:
            fn = os.path.abspath(fn)
            annotation.filename = fn
        elif treat_image == ImageTreatmentSetting.REWRITE_RELPATH:
            fn = os.path.relpath(fn, self.image_dir)
            annotation.filename = fn
        elif treat_image == (ImageTreatmentSetting.COPY_IMAGE, ImageTreatmentSetting.COPY_IMAGE_RENAME):
            os.makedirs(self.image_dir, exist_ok=True)
            dest_fn = os.path.split(fn)[1]
            if treat_image == ImageTreatmentSetting.COPY_IMAGE_RENAME:
                dest_fn = f'{self.rename_counter:06}' + os.path.splitext(dest_fn)[1]
                annotation.id = f'{self.rename_counter:06}'
            dest_pth = os.path.join(self.image_dir, dest_fn)
            shutil.copy2(fn, dest_pth)
            annotation.filename = os.path.relpath(dest_pth, self.image_dir)
        elif treat_image in (ImageTreatmentSetting.SYMLINK_IMAGE, ImageTreatmentSetting.SYMLINK_IMAGE_RENAME):
            os.makedirs(self.image_dir, exist_ok=True)
            dest_fn = os.path.split(fn)[1]
            if treat_image == ImageTreatmentSetting.SYMLINK_IMAGE_RENAME:
                dest_fn = f'{self.rename_counter:06}' + os.path.splitext(dest_fn)[1]
                annotation.id = f'{self.rename_counter:06}'
            dest_pth = os.path.join(self.image_dir, dest_fn)
            if os.path.islink(dest_pth):
                os.unlink(dest_pth)
            os.symlink(os.path.abspath(fn), dest_pth)
            annotation.filename = os.path.relpath(dest_pth, self.image_dir)
        annotation.write(os.path.join(self.annotation_output_dir, annotation.id + ".xml"))
        for o in annotation.objects:
            self._log_object(o.name)
        self.ids.append(annotation.id)

    def write_lineage(self, d: DataLineage):
        d.dump_yaml(os.path.join(self.root_dir, "data-lineage.yaml"))
    
    def check_lineage_okay(self, d: DataLineage):
        pth = os.path.join(self.root_dir, "data-lineage.yaml")
        if not os.path.isfile(pth):
            return False
        other = DataLineage(pth)
        return other.is_uptodate_with(d)
        


    def write_dataset_meta(self):
        for sl in ["Main","Action","Segmentation","Layout"]:
            os.makedirs(os.path.join(self.root_dir, "ImageSets", sl), exist_ok=True)
            with open(os.path.join(self.root_dir, "ImageSets", sl,"default.txt"),"w") as f:
                f.write("\n".join(self.ids))
        for lbl in self.metrics.keys():
            with open(os.path.join(self.root_dir,"ImageSets","Main","{lbl}_default.txt".format(lbl=lbl)),"w") as f:
                f.write("")
        with open(os.path.join(self.root_dir,"labelmap.txt"),"w") as f:
            f.write("\n".join(["{x}:0,0,0::".format(x=x) for x in self.metrics.keys()]))



class AnnotationZip(object):
    def __init__(self, zipfile: Union[str, IO, pathlib.Path], root_dir: str=  None) -> None:
        self.zipfile = zipfile
        if isinstance(zipfile, str):
            if not os.path.exists(zipfile):
                raise Exception(f"file not found {zipfile}")
        if isinstance(zipfile, pathlib.Path):
            if not zipfile.exists():
                raise Exception(f"file {zipfile} not found")
        self.root_dir = root_dir
    
    def as_lineage_source(self):
        src = LineageSource()
        src.root_dir = self.root_dir
        src.source_hash = hash_from_file(self.zipfile)
        if self.root_dir is None:
            src.root_dir = os.getcwd()
        if isinstance(self.zipfile, str):
            src.annotation_path = self.zipfile
        else:
            src.annotation_path = self.zipfile.name
        return src

    def generate_annotations(self) -> Generator[PascalVocAnnotation, None, None]:
        with zipfile.ZipFile(self.zipfile) as zip:
            for info in zip.infolist():
                if pathlib.Path(info.filename).suffix.lower() == '.xml':
                    try:
                        fobj = zip.open(info.filename)
                        f = ET.parse(zip.open(info.filename))
                        yield PascalVocAnnotation(f, info.filename, self.root_dir)
                    finally:
                        fobj.close()

def get_zip_annotations(zipfile: Union[str, IO], root_dir: str = None) -> Generator[PascalVocAnnotation, None, None]:
    az = AnnotationZip(zipfile, root_dir)
    return az.generate_annotations()

class AnnotationDirectory(object):
    def __init__(self, path: str) -> None:
        self.path = path

    def as_lineage_source(self):
        if os.path.isfile(os.path.join(self.path,'data-lineage.yaml')):
            src = DataLineage(os.path.join(self.path,'data-lineage.yaml')).as_source()
        else:
            src = LineageSource()
        
        src.root_dir = str(self.path.absolute())
        return src

    def generate_annotations(self) -> Generator[PascalVocAnnotation, None, None]:
        for root,dirs,files in os.walk(self.path):
            for f in files:
                if os.path.splitext(f)[1].lower() == '.xml':
                    yield PascalVocAnnotation(os.path.join(root,f), f, self.path)

def get_dir_annotations(path: str) -> Generator[PascalVocAnnotation, None, None]:
    ad = AnnotationDirectory(path)
    return ad.generate_annotations()

def filter_args_for_datalineage(args: dict):
    allowed = [(int, False), (str, False), (pathlib.Path, True), (bool, False), (pathlib.PosixPath, True)]
    new_d = {}
    for (k,v) in args.items():
        for (a,to_str) in allowed:
            if isinstance(v,a):
                if to_str:
                    v = str(v)
                new_d[k] = v
    return new_d
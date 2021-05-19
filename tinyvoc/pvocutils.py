import xml.etree.ElementTree as ET
from typing import List, Dict, IO, Optional, Union, Generator
import zipfile
import os, shutil
from enum import Enum
import logging, pathlib
import yaml

class LineageSource(object):
    def __init__(self):
        self.annotation_path = ''
        self.image_path = ''
        self.root_dir = ''
        self.sources = []
        pass
    
    def _load_from_dict(self, dct):
        self.annotation_path = dct['annotation_path']
        self.image_path = dct['image_path']
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
        d['sources'] = [ x._to_dict() for x in self.sources ]
        return d

class DataLineage(object):
    def __init__(self, src: Optional[Union[str, IO]] = None) -> None:
        self.data = { 'sources' : [] }
        if isinstance(src, str):
            src = open(src, 'r')
        if src is None:
            pass
        else:
            self.data = yaml.load(src, yaml.Loader)
    
    @property
    def sources(self) -> List[LineageSource]:
        d = self.data
        def create_src(d) -> LineageSource:
            l = LineageSource()
            l._load_from_dict(d)
            return l
        return [create_src(x) for x in d['sources']]
    

    def dump_yaml(self, path: str):
        with open(path,'w') as f:
            f.write(yaml.dump(self.data, Dumper=yaml.Dumper))

    @sources.setter
    def sources(self, s: List[LineageSource]):
        self.data['sources'] = [x._to_dict() for x in s]
    
    def as_source(self) -> LineageSource:
        l = LineageSource()
        l.sources = self.sources
        return l

    def add_source(self, src):
        s = self.sources
        s.append(src)
        self.sources = s



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
        os.makedirs(self.image_dir, exist_ok=True)
        os.makedirs(self.annotation_output_dir, exist_ok=True)
        self.rename_counter = 0
        self.metrics = {}
        self.ids = []

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
        if os.path.isfile(fn):
            img_path = fn
        elif os.path.isfile(os.path.join(src_root_dir, fn)):
            img_path = os.path.join(src_root_dir, fn)
        elif os.path.isfile(os.path.join(src_root_dir, "JPEGImages", fn)):
            img_path = os.path.join(src_root_dir, "JPEGImages", fn)
        else:
            logging.debug(f"0 image does not exist {annotation.id} --> try to find {fn}")
            return None
        fn = img_path

        self.rename_counter += 1
        if treat_image == ImageTreatmentSetting.REWRITE_ABSPATH:
            fn = os.path.abspath(fn)
            annotation.filename = fn
        elif treat_image == ImageTreatmentSetting.REWRITE_RELPATH:
            fn = os.path.relpath(fn, self.image_dir)
            annotation.filename = fn
        elif treat_image == (ImageTreatmentSetting.COPY_IMAGE, ImageTreatmentSetting.COPY_IMAGE_RENAME):
            dest_fn = os.path.split(fn)[1]
            if treat_image == ImageTreatmentSetting.COPY_IMAGE_RENAME:
                dest_fn = f'{self.rename_counter:06}' + os.path.splitext(dest_fn)[1]
                annotation.id = f'{self.rename_counter:06}'
            dest_pth = os.path.join(self.image_dir, dest_fn)
            shutil.copy2(fn, dest_pth)
            annotation.filename = os.path.relpath(dest_pth, self.image_dir)
        elif treat_image in (ImageTreatmentSetting.SYMLINK_IMAGE, ImageTreatmentSetting.SYMLINK_IMAGE_RENAME):
            dest_fn = os.path.split(fn)[1]
            if treat_image == ImageTreatmentSetting.SYMLINK_IMAGE_RENAME:
                dest_fn = f'{self.rename_counter:06}' + os.path.splitext(dest_fn)[1]
                annotation.id = f'{self.rename_counter:06}'
            dest_pth = os.path.join(self.image_dir, dest_fn)
            os.symlink(os.path.abspath(fn), dest_pth)
            annotation.filename = os.path.relpath(dest_pth, self.image_dir)
        annotation.write(os.path.join(self.annotation_output_dir, annotation.id + ".xml"))
        for o in annotation.objects:
            self._log_object(o.name)
        self.ids.append(annotation.id)

    def write_lineage(self, d: DataLineage):
        d.dump_yaml(os.path.join(self.root_dir, "data-lineage.yaml"))

    def write_dataset_meta(self):
        for sl in ["Main","Action","Segmentation","Layout"]:
            os.makedirs(os.path.join(self.root_dir, "ImageSets", sl))
            with open(os.path.join(self.root_dir, "ImageSets", sl,"default.txt"),"w") as f:
                f.write("\n".join(self.ids))
        for lbl in self.metrics.keys():
            with open(os.path.join(self.root_dir,"ImageSets","Main","{lbl}_default.txt".format(lbl=lbl)),"w") as f:
                f.write("")
        with open(os.path.join(self.root_dir,"labelmap.txt"),"w") as f:
            f.write("\n".join(["{x}:0,0,0::".format(x=x) for x in self.metrics.keys()]))



class AnnotationZip(object):
    def __init__(self, zipfile: Union[str, IO], root_dir: str=  None) -> None:
        self.zipfile = zipfile
        self.root_dir = root_dir
    
    def as_lineage_source(self):
        src = LineageSource()
        src.root_dir = self.root_dir
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

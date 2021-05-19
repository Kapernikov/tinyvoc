# Tiny voc utilities

This package contains some wrapper classes for Pascal VOC annotation datasets for object detection + some very simple utilities.
The goal of this package is to write simple data preparation utilities for pascalvoc (eg merging, filtering, ...). Datumaro is a more complete solution, but if the goal is to write a minimal python utility, this package might do the job.

## Installation

```shell
pip install git+https://github.com/Kapernikov/tinyvoc
```

## Utilities

### merge-annotations

The goal of this utility is to merge different pascalVOC annotation directories. It does it by rewriting the XML annotations so that the filenames are unique, and symlinking the images (a trivial change in the source code can make it copy instead of symlink). Can be used in a DVC pipeline

### prepare-annotations

This utility takes a CVAT pascalvoc export zip, unzips it, while filtering out empty annotations, and annotations for labels that we don't need. It is meant to be used as part of a DVC pipeline, using a parameters file like this:

```yaml
annotations:
  # whether to add the "type" attribute (you can create it in CVAT) to the class names of the objects
  concat-type-attribute: False
  # valid labels
  valid-labels:
    - Pedestrian
    - Bicycle
    - Car
```

Changes in the parameters file can easily be tracked by DVC. This utility also writes metrics that can easily be consumed by DVC.

### video-to-frame

Small wrapper around ffmpeg (please install ffmpeg) that will convert a video to individual frames, while respecting the filename convention of CVAT.
This way, if you use CVAT to annotate a video, you can use video-to-frame followed by prepare-annotations on the pascalvoc export


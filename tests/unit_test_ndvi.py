import unittest, os, sys, glob, shutil, argparse, logging, math
import gdal, ogr, osr, gdalconst
import numpy as np

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
root_dir = os.path.dirname(script_dir)
sys.path.append(root_dir)

from lib import mosaic

logger = logging.getLogger("logger")
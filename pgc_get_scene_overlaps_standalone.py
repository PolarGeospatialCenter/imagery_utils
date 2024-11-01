import argparse
import math
import os
import re
from collections import namedtuple

import gdal
import gdalconst
import ogr
import osr

# common name id, attribute field name, storage type, field width, field precision
CoordinateMap = namedtuple("CoordinateMap", ('ul', 'ur', 'lr', 'll'))

wgs84 = 4326


def main():
    #############################
    ######  Parse Arguments
    #############################

    #### Set Up Arguments

    parser = argparse.ArgumentParser(description="Build of text file of stereopair scene overlaps")

    #### Positional Arguments
    parser.add_argument('src', help="source directory")
    parser.add_argument('dst', help="destination text file")

    #### Parse Arguments
    args = parser.parse_args()

    src = os.path.abspath(args.src)
    if not os.path.isdir(src):
        parser.error("src must be a valid directory")

    dst = os.path.abspath(args.dst)
    if not os.path.isdir(os.path.dirname(dst)):
        parser.error("dst must be in an existing directory")
    if os.path.isdir(dst):
        parser.error("dst must be a file")

    ###########################################
    #####  Sort Images into Overlaps
    ###########################################

    epsg = 4326
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(epsg)

    print("Source: %s" % src)
    stereo_images = []

    #### Look for images (case insensitive), parent dir is pairname
    scenes = {}
    for root, dirs, files in os.walk(src):
        for f in files:
            if f.endswith((".NTF", ".ntf", ".TIF", ".tif")) and 'browse' not in f.lower():
                # "P1BS" in f and
                # print f

                ## check for duplicate scene IDs (both a nitf and a tiff file of same base name) and prefer ntfs
                sceneid, ext = os.path.splitext(f)
                if sceneid in scenes:
                    if ext.lower() == '.ntf':  # if extension is ntf, replace whatever is already there
                        scenes[sceneid] = os.path.abspath(os.path.join(root, f))
                else:
                    scenes[sceneid] = os.path.abspath(os.path.join(root, f))

    for sceneid in scenes:
        scenepath = scenes[sceneid]
        pairname = os.path.basename(os.path.dirname(scenepath))
        ## Build RasterInfo object

        ##  Build StereoImage object. Catid is not known
        stereo_image = StereoImage(scenepath, pairname, srs)

        if stereo_image.isvalid is True:
            if stereo_image.source.bands == 1:  # return only Pan images
                stereo_images.append(stereo_image)
            else:
                print("Stereo image rejected due to band constraint: %s has %d bands" % (stereo_image.path,
                                                                                         stereo_image.source.bands))
        else:
            print("Cannot gather sufficient metadata for stereo image: %s" % (stereo_image.path))

    pn_img_dict = {}  # dictionary of pairname with StereoImages list
    stereopairs = []
    # print len(stereo_images)
    if len(stereo_images) > 0:
        # sort into pairname dict
        for img in stereo_images:
            if img.pairname not in pn_img_dict:
                pn_img_dict[img.pairname] = []
                pn_img_dict[img.pairname].append(img)

        # print pn_img_dict
        for pairname in pn_img_dict:
            stereopair = StereoPair(pairname, pn_img_dict[pairname], "scene")
            if stereopair.isvalid is True:
                stereopairs.append(stereopair)

    print("Number of stereo pairs: %i" % len(stereopairs))
    txt = open(dst, 'w')

    for stereopair in stereopairs:
        i = 0
        print("Stereo pair: %s" % stereopair)

        print("Number of overlaps: %i" % len(stereopair.overlaps))
        overlaps = sorted(stereopair.overlaps, key=lambda olap: olap.name)
        for overlap in overlaps:
            overlap_geom = overlap.geom.Clone()

            ### overlap area in meters
            if srs.GetLinearUnitsName != 'Meter':
                overlap_centroid = overlap_geom.Centroid()
                srs_wgs84 = osr.SpatialReference()
                srs_wgs84.ImportFromEPSG(wgs84)
                if not srs.IsSame(srs_wgs84):
                    to_wgs84_transform = osr.CoordinateTransformation(srs, wgs84)
                    overlap_centroid.Transform(to_wgs84_transform)
                lon = overlap_centroid.GetX()
                lat = overlap_centroid.GetY()
                utm = int((lon + 180) // 6 + 1)
                if lat >= 0:
                    epsg_utm = 32600 + utm
                else:
                    epsg_utm = 32700 + utm

                srs_utm = osr.SpatialReference()
                srs_utm.ImportFromEPSG(epsg_utm)

                to_utm_transform = osr.CoordinateTransformation(srs, srs_utm)
                overlap_geom.Transform(to_utm_transform)

            area_in_meters = overlap_geom.Area()

            i += 1

            txt.write("{} {} {} {} {}\n".format(
                i,
                overlap.name,
                overlap.images1[0].path,
                overlap.images2[0].path,
                area_in_meters / 1000000.0
            ))

    txt.close()


class RasterInfo(object):
    """
    docstring
    """

    def __str__(self):
        return self.name

    def __init__(self, name, geom, srs, bands, elev=None, pixelcount=None, linecount=None, xres=None, yres=None,
                 coords=None):

        self.name = name
        self.geom = geom
        self.srs = srs
        self.bands = bands
        self.elev = elev
        self.pixelcount = pixelcount
        self.linecount = linecount
        self.xres = xres
        self.yres = yres
        self.coords = coords

    def transform(self, target_srs):

        target_geom = self.geom.Clone()
        ul = self.coords.ul.Clone()
        ur = self.coords.ur.Clone()
        lr = self.coords.lr.Clone()
        ll = self.coords.ll.Clone()

        if not self.srs.IsSame(target_srs):
            #### Create srs object
            src_tgt_coordtf = osr.CoordinateTransformation(self.srs, target_srs)

            #### Transform geometry to target srs

            try:
                target_geom.Transform(src_tgt_coordtf)
                ul.Transform(src_tgt_coordtf)
                ur.Transform(src_tgt_coordtf)
                lr.Transform(src_tgt_coordtf)
                ll.Transform(src_tgt_coordtf)

            except TypeError as e:
                print("%s Cannot Transform Geometry for image %s: %s" % (e, self.name, self.geom))
                return None

        #### Add bit to calculate coords from geom

        #### calc tgt res
        if self.pixelcount and self.linecount and self.coords:

            target_coords = CoordinateMap(ul, ur, lr, ll)

            target_xres = abs(math.sqrt((ul.GetX() - ur.GetX()) ** 2 + (ul.GetY() - ur.GetY()) ** 2) / self.pixelcount)
            target_yres = abs(math.sqrt((ul.GetX() - ll.GetX()) ** 2 + (ul.GetY() - ll.GetY()) ** 2) / self.linecount)

        else:
            target_coords = None
            target_xres = None
            target_yres = None

        # print "Rasterinfo: target xres = %s, target yres = %s" %(target_xres, target_yres)

        target = RasterInfo(
            self.name,
            target_geom,
            target_srs,
            self.bands,
            self.elev,
            self.pixelcount,
            self.linecount,
            target_xres,
            target_yres,
            target_coords
        )

        return target


class StereoImage(object):
    """
    docstring
    """

    def __str__(self):
        return self.basename

    def __init__(self, image_path, pairname, tgt_srs, rasterinfo=None):

        self.path = image_path
        self.pairname = pairname
        self.dir, self.name = os.path.split(image_path)
        self.basename, self.file_ext = os.path.splitext(self.name)
        self.tgt_srs = tgt_srs
        self.isvalid = True
        # print self.path

        #### If rasterinfo is provided, use it. Otherwise
        #### get the geometry and attributes from the raster
        if rasterinfo:
            self.source = rasterinfo
        else:
            if os.path.isfile(image_path):
                self.source = self._getRasterInfo()
            else:
                raise RuntimeError("Cannot read source image path: %s" % image_path)

        if self.source:
            self.target = self.source.transform(tgt_srs)

            ## Find metadata file (xml/pvl/txt)
            if os.path.isfile(os.path.join(self.dir, self.basename + ".XML")):
                self.metapath = os.path.join(self.dir, self.basename + ".XML")
            ## if files are not on an available filesystem, xml path
            # is assumed to be the same as the file path with a lower case extension
            else:
                self.metapath = os.path.join(self.dir, self.basename + ".xml")

            #### If Catid is provided, use it. Otherwise get the catid from
            #### the raster metadata file (xml/pvl,txt)
            self.catid, self.order_id, self.tile = getCatidFromName(self.name)

            #### Check if stereo image object has enough valid values
            if self.metapath is None or self.target is None or self.catid is None:
                self.isvalid = False
                print("StereoImage is not valid. One of the following is None:")
                print("metapath:", self.metapath)
                print("target geom:", self.target.geom)
                print("catid:", self.catid)
        else:
            self.isvalid = False

    def _getRasterInfo(self):

        geom = None
        srs = None

        try:
            ds = gdal.Open(self.path, gdalconst.GA_ReadOnly)
            if ds is not None:
                ####  Get extent from GCPs
                num_gcps = ds.GetGCPCount()
                bands = ds.RasterCount
                xsize = ds.RasterXSize
                ysize = ds.RasterYSize
                proj = ds.GetProjectionRef()
                m = ds.GetMetadata("RPC")
                if "HEIGHT_OFF" in m:
                    elev = m["HEIGHT_OFF"]
                    elev = float(''.join([c for c in elev if c in '1234567890.+-']))
                else:
                    elev = 0.0

                if num_gcps == 4:
                    gcps = ds.GetGCPs()
                    proj = ds.GetGCPProjection()

                    gcp_dict = {}

                    id_dict = {"UpperLeft": 1,
                               "1": 1,
                               "UpperRight": 2,
                               "2": 2,
                               "LowerLeft": 4,
                               "4": 4,
                               "LowerRight": 3,
                               "3": 3}

                    for gcp in gcps:
                        gcp_dict[id_dict[gcp.Id]] = [float(gcp.GCPPixel), float(gcp.GCPLine), float(gcp.GCPX),
                                                     float(gcp.GCPY), float(gcp.GCPZ)]

                    ulx = gcp_dict[1][2]
                    uly = gcp_dict[1][3]
                    urx = gcp_dict[2][2]
                    ury = gcp_dict[2][3]
                    llx = gcp_dict[4][2]
                    lly = gcp_dict[4][3]
                    lrx = gcp_dict[3][2]
                    lry = gcp_dict[3][3]

                else:
                    gtf = ds.GetGeoTransform()

                    ulx = gtf[0] + 0 * gtf[1] + 0 * gtf[2]
                    uly = gtf[3] + 0 * gtf[4] + 0 * gtf[5]
                    urx = gtf[0] + xsize * gtf[1] + 0 * gtf[2]
                    ury = gtf[3] + xsize * gtf[4] + 0 * gtf[5]
                    llx = gtf[0] + 0 * gtf[1] + ysize * gtf[2]
                    lly = gtf[3] + 0 * gtf[4] + ysize * gtf[5]
                    lrx = gtf[0] + xsize * gtf[1] + ysize * gtf[2]
                    lry = gtf[3] + xsize * gtf[4] + ysize * gtf[5]

                ul = ogr.Geometry(ogr.wkbPoint)
                ul.AddPoint(ulx, uly)
                ur = ogr.Geometry(ogr.wkbPoint)
                ur.AddPoint(urx, ury)
                lr = ogr.Geometry(ogr.wkbPoint)
                lr.AddPoint(lrx, lry)
                ll = ogr.Geometry(ogr.wkbPoint)
                ll.AddPoint(llx, lly)

                coords = CoordinateMap(ul, ur, lr, ll)

                xres = abs(math.sqrt((ulx - urx) ** 2 + (uly - ury) ** 2) / xsize)
                yres = abs(math.sqrt((ulx - llx) ** 2 + (uly - lly) ** 2) / ysize)

                ####  Create geometry object
                ring = ogr.Geometry(ogr.wkbLinearRing)
                ring.AddPoint(ulx, uly)
                ring.AddPoint(urx, ury)
                ring.AddPoint(lrx, lry)
                ring.AddPoint(llx, lly)
                ring.AddPoint(ulx, uly)
                geom = ogr.Geometry(ogr.wkbPolygon)
                geom.AddGeometry(ring)
                # print proj

                #### Create srs objects
                srs = osr.SpatialReference(proj)

                ##### build rasterinfo class and return
                rasterinfo = RasterInfo(self.path, geom, srs, bands, elev, xsize, ysize, xres, yres, coords)
                return rasterinfo

        except Exception as e:
            print("Exception in _getRasterInfo: {}".format(e))
            return None


class StereoPair(object):
    """
    docstring
    """

    def __init__(self, pairname, stereo_images, sp_type):

        # print pairname
        self.pairname = pairname
        self.stereo_images = stereo_images
        self.sp_type = sp_type
        self.isvalid = True
        self.overlaps = []

        self.catids = list(set([img.catid for img in stereo_images]))
        self.catids.sort()
        # print self.catids
        self.images = [img.path for img in stereo_images]

        if len(self.catids) == 2:
            if self.sp_type == "mosaic":
                self.overlaps = self._buildOverlaps_mosaic()
            elif self.sp_type == "scene":
                self.overlaps = self._buildOverlaps_scene()
            else:
                print("Incorrect value for stereopair type (%s)" % self.sp_type)
                self.isvalid = False
        else:
            print("Incorrect number of component catids (%i)" % len(self.catids))
            self.isvalid = False

        if len(self.overlaps) == 0:
            print("no overlaps found in pair: %s" % self.pairname)
            self.isvalid = False

        rasters_band_counts = set([img.source.bands for img in self.stereo_images])
        if len(rasters_band_counts) != 1:
            print("Images in this stereopair have varying numbers of bands.  This will break things."
                  "  Try running the script with the band_constraint option.\n\t{pairname} - {bandset}".format(
                pairname=self.pairname,
                bandset=str(rasters_band_counts)
            ))
            self.isvalid = False

    def writeIndex(self, dstdir, srs):
        OGR_DRIVER = "ESRI Shapefile"
        ogrDriver = ogr.GetDriverByName(OGR_DRIVER)

        #### Make pairname destination folder
        if not os.path.isdir(dstdir):
            os.makedirs(dstdir)

        shp = os.path.join(dstdir, self.pairname)
        if os.path.isfile(shp + '.shp'):
            ogrDriver.DeleteDataSource(shp + '.shp')

        shapefile = ogrDriver.CreateDataSource(shp + '.shp')

        if shapefile is not None:
            shpn = os.path.basename(shp)
            layer = shapefile.CreateLayer(shpn, srs, ogr.wkbPolygon)

            field = ogr.FieldDefn("overlap", ogr.OFTString)
            field.SetWidth(250)
            layer.CreateField(field)

            field = ogr.FieldDefn("perc_ol", ogr.OFTReal)
            layer.CreateField(field)

            for overlap in self.overlaps:
                feature = ogr.Feature(layer.GetLayerDefn())
                feature.SetField("overlap", overlap.name)
                feature.SetField("perc_ol", overlap.overlap_percent)
                feature.SetGeometry(overlap.geom)

                layer.CreateFeature(feature)

        else:
            print("Cannot create shapefile: %s" % shp)

    def getOverlapsByName(self, overlap_name):

        named_overlaps = []
        for overlap in self.overlaps:
            if overlap.name == overlap_name:
                named_overlaps.append(overlap)

        if len(named_overlaps) == 0:
            print("Error: Cannot locate matching overlaps: %s" % overlap_name)
        elif len(named_overlaps) == 1:
            return named_overlaps[0]
        elif len(named_overlaps) > 1:
            return named_overlaps

    def _buildOverlaps_mosaic(self):

        catid_geoms = {}  # dict of catid to union of all geoms with that catid
        catid_images = {}  # dict of catid to a list of StereoImage objects

        for img in self.stereo_images:
            if img.catid in catid_images:
                imglist = catid_images[img.catid]
                imglist.append(img)
                catid_images[img.catid] = imglist

                catid_geoms[img.catid] = catid_geoms[img.catid].Union(img.target.geom)
            else:
                catid_images[img.catid] = [img]
                catid_geoms[img.catid] = img.target.geom

        overlap_images = (catid_images[self.catids[0]], catid_images[self.catids[1]])
        # print overlap_images[0]
        # print overlap_images[1]
        geom1 = catid_geoms[self.catids[0]]
        geom2 = catid_geoms[self.catids[1]]
        overlap_geom = geom1.Intersection(geom2)
        union_geom = geom1.Union(geom2)
        overlap_percent = overlap_geom.GetArea() / union_geom.GetArea()

        overlap = Overlap(overlap_images, overlap_geom, overlap_percent, overlap_geom.Area(), self.pairname,
                          self.sp_type, self.pairname)

        return [overlap]

    def _buildOverlaps_scene(self):

        overlaps = []

        ### Add code to get min xres and yres from images

        for img1 in self.stereo_images:
            if img1.catid == self.catids[0]:
                for img2 in self.stereo_images:
                    if img2.catid != img1.catid:
                        #### compare geoms
                        if img1.target.geom.Intersects(img2.target.geom):
                            # print img1.name,img2.name

                            overlap_images = ([img1], [img2])
                            overlap_geom = img1.target.geom.Intersection(img2.target.geom)
                            union_geom = img1.target.geom.Union(img2.target.geom)
                            overlap_percent = overlap_geom.GetArea() / union_geom.GetArea()
                            # overlap_name = "%s_%s" %(img1.basename, img2.basename)
                            # Overlap name:
                            # WV01_20120603_102001001B4BFB00_102001001BE7F800_R1C1-052903570060_01_P001_052903564030_01_P001
                            img1_identifier = "{1}-{0}".format(img1.order_id, img1.tile) if img1.tile else img1.order_id
                            img2_identifier = "{1}-{0}".format(img2.order_id, img2.tile) if img2.tile else img2.order_id
                            overlap_name = "{}_{}_{}".format(self.pairname, img1_identifier, img2_identifier)

                            if overlap_percent >= 0.10:
                                overlap = Overlap(overlap_images, overlap_geom, overlap_percent, overlap_geom.Area(),
                                                  overlap_name, self.sp_type, self.pairname)

                                if overlap is not None:
                                    overlaps.append(overlap)

        return overlaps

    def __str__(self):
        return self.pairname


class Overlap(object):
    """
    docstring
    """

    def __str__(self):
        return self.name

    def __init__(self, images_list, geom, overlap_percent, overlap_area, overlap_name, overlap_type, pairname):

        self.geom = geom
        self.bbox = geom.GetEnvelope()  # Get Envelope returns a tuple (minX, maxX, minY, maxY)
        self.overlap_type = overlap_type
        self.images1 = images_list[0]
        self.images2 = images_list[1]
        self.name = overlap_name
        self.pairname = pairname
        self.overlap_percent = overlap_percent
        self.overlap_area = overlap_area
        self.overlap_filename = "%s.overlap" % overlap_name
        self.overlap_geojson = "%s.geojson" % overlap_name
        self.dem_name = "%s-DEM.tif" % overlap_name
        self.pc_name = "%s-PC.tif" % overlap_name

        #### Get minimum res from Image objects: If image res values are None, then xres and yres will also be None
        all_images = self.images1 + self.images2
        self.xres = min([si.target.xres for si in all_images])
        self.yres = min([si.target.yres for si in all_images])

        try:
            self.elev = sum([si.target.elev for si in all_images]) / float(len(all_images))
        except TypeError:
            self.elev = 0.0

    ##### Write overlap index files
    def writeOverlapFile(self, dstdir):

        #### Make pairname destination folder
        pairname_dstdir = dstdir
        if not os.path.isdir(pairname_dstdir):
            os.makedirs(pairname_dstdir)

        #### Write .overlap file
        f = open(os.path.join(pairname_dstdir, self.overlap_filename), 'w')
        f.write("IMAGE;CATALOGID;PAIRNAME;OVERLAP_TYPE\n")

        images = self.images1 + self.images2

        for image in images:
            vals = {
                "pairname": self.pairname,
                "overlap_type": self.overlap_type,
                "image": image.path,
                "catid": image.catid,
            }
            f.write("{image};{catid};{pairname};{overlap_type}\n".format(**vals))

        f.close()


def getCatidFromName(filename):
    PGC_DG_FILE = re.compile(r"""
                         (?P<pgcpfx>                        # PGC prefix
                            (?P<sensor>[a-z]{2}\d{2})_      # Sensor code
                            (?P<tstamp>\d{14})_             # Acquisition time (yyyymmddHHMMSS)
                            (?P<catid>[a-f0-9]{16})         # Catalog ID
                         )_
                         (?P<oname>                         # Original DG name
                            (?P<ts>\d{2}[a-z]{3}\d{8})-     # Acquisition time (yymmmddHHMMSS)
                            (?P<prod>[a-z0-9]{4})_?         # DG product code
                            (?P<tile>R\d+C\d+)?-            # Tile code (mosaics, optional)
                            (?P<oid>                        # DG Order ID
                                (?P<onum>\d{12}_\d{2})_     # DG Order number
                                (?P<pnum>P\d{3})            # Part number
                            )
                            (?P<tail>[a-z0-9_-]+(?=\.))?    # Descriptor (optional)
                         )
                         (?P<ext>\.[a-z0-9][a-z0-9.]*)      # File name extension
                         """, re.I | re.X)

    # WV01_20120603220928_102001001B4BFB00_12JUN03220928-P1BS_R1C1-052903570060_01_P001.tif
    # WV01_20120603221007_102001001BE7F800_12JUN03221007-P1BS-052903564030_01_P001.tif

    match = PGC_DG_FILE.search(filename)
    if match is not None:
        grp_dct = match.groupdict()
        catid = grp_dct['catid']
        order_id = grp_dct['oid']
        if 'tile' in grp_dct:
            tile = grp_dct['tile']
        else:
            tile = None

        return catid, order_id, tile

    else:
        return None, None, None


if __name__ == '__main__':
    main()

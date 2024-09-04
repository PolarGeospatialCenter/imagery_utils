import argparse
import logging
import re
import shlex
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path


def get_logger(logfile: Path, logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s- %(message)s", datefmt="%m-%d-%Y %H:%M:%S"
    )

    file_handler = logging.FileHandler(filename=logfile, mode="a")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    logger.addHandler(stream_handler)
    return logger


@dataclass
class CLIArgs:
    input_dir: Path
    output_dir: Path


def validate_cli_args(args: CLIArgs) -> CLIArgs:
    """Validate the CLI arguments.

    Requirements:
    - The input directory must exist and must be a directory"""
    if not args.input_dir.exists():
        raise FileNotFoundError(
            f"The provided input-dir does not exist: {args.input_dir}"
        )
    if not args.input_dir.is_dir():
        raise NotADirectoryError(
            f"The provided input-dir is not a directory: {args.input_dir}"
        )
    return CLIArgs


def get_cli_args() -> CLIArgs:
    """Parse and validate commandline arguments, returning a CLIArgs instance if valid"""
    parser = argparse.ArgumentParser(
        prog="stack_landsat",
        description="Utility for combining Landsat bands 4, 3, 2 into a multi-band RGB TIF",
    )
    parser.add_argument(
        "--input-dir",
        "-i",
        type=Path,
        required=True,
        help="Directory containing Landsat TIFs with one band per TIF [REQUIRED]",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=Path,
        required=True,
        help="Directory to write multi-band TIFs. The directory will be created if it does not exist. [REQUIRED]",
    )
    try:
        return validate_cli_args(parser.parse_args(namespace=CLIArgs))
    except (FileNotFoundError, NotADirectoryError) as e:
        print(f"ERROR: {e.args[0]}")
        exit()


def strip_band_from_scene_name(scene_name: str) -> str:
    """Removes the band designation (e.g. '_B2') from the Landsat scene name"""
    return re.sub("_B[0-9]", "", scene_name)


def create_rgb_tif(input_dir: Path, output_dir: Path, scene_name: str) -> None:
    red_band = input_dir / f"{scene_name}_B4.TIF"
    green_band = input_dir / f"{scene_name}_B3.TIF"
    blue_band = input_dir / f"{scene_name}_B2.TIF"
    temp_vrt = output_dir / "temp.vrt"
    rgb_tif = output_dir / f"{scene_name}_RGB.TIF"

    missing_bands = []
    if not red_band.exists():
        missing_bands.append(str(red_band))
    if not green_band.exists():
        missing_bands.append(str(green_band))
    if not blue_band.exists():
        missing_bands.append(str(blue_band))
    if len(missing_bands) > 0:
        raise FileNotFoundError(
            f"The following band TIFs were not found: {missing_bands}"
        )

    gdalbuildvrt_cmd = f"gdalbuildvrt -separate -overwrite {temp_vrt} {red_band} {green_band} {blue_band}"
    gdal_translate_cmd = f"gdal_translate -f COG {temp_vrt} {rgb_tif}"

    try:
        subprocess.run(shlex.split(gdalbuildvrt_cmd, posix=False), check=True)
        subprocess.run(shlex.split(gdal_translate_cmd, posix=False), check=True)
    finally:
        temp_vrt.unlink()


def cli() -> None:
    args = get_cli_args()

    logfile = args.input_dir / f"stack_landsat_{date.today()}.log"
    logger = get_logger(
        logfile=logfile,
        logger_name=__file__,
    )

    logger.info("-" * 80)
    logger.info(f"Input directory: {args.input_dir}")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Logfile: {logfile}")
    logger.info("-" * 80)

    if not args.output_dir.exists():
        logger.info(f"Output directory does not exist. Creating output directory.")
        args.output_dir.mkdir(parents=True)

    logger.info(f"Scanning input directory for Landsat scenes")
    tifs = list(args.input_dir.glob("*_B[0-9].TIF"))
    scene_names = {strip_band_from_scene_name(tif.stem) for tif in tifs}
    if len(scene_names) == 0:
        logging.error("No scenes found in input directory")
        logging.error("Exiting process...")
        return

    logger.info(f"Number of scenes found to process: {len(scene_names)}")
    succeeded_scenes = []
    failed_scenes = []
    for scene_name in scene_names:
        logger.info(f"Processing scene: {scene_name}")
        try:
            create_rgb_tif(
                input_dir=args.input_dir,
                output_dir=args.output_dir,
                scene_name=scene_name,
            )
            succeeded_scenes.append(scene_name)
        except FileNotFoundError as e:
            logger.error(e.args[0])
            logger.error(f"Skipping scene: {scene_name}")
            failed_scenes.append(scene_name)
            continue

    logger.info(f"Number of scenes successfully processed: {len(succeeded_scenes)}")
    logger.info(f"Number of scenes that failed to process: {len(failed_scenes)}")
    if len(failed_scenes) > 0:
        logger.warning("Scenes that failed to process:")
        for scene_name in failed_scenes:
            logger.warning(f"\t{scene_name}")


if __name__ == "__main__":
    cli()

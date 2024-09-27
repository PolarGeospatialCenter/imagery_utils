#!/usr/bin/env python

import argparse
import configparser
import getpass
import logging
import os
import platform
import subprocess
import sys
import textwrap

# Script paths
SCRIPT_FILE = os.path.abspath(os.path.realpath(__file__))
SCRIPT_FNAME = os.path.basename(SCRIPT_FILE)
SCRIPT_NAME, SCRIPT_EXT = os.path.splitext(SCRIPT_FNAME)
SCRIPT_DIR = os.path.dirname(SCRIPT_FILE)

# Global vars
DEFAULT_CONFIG_FILE = os.path.join(SCRIPT_DIR, f'{SCRIPT_NAME}_config.ini')
if platform.system().lower() == 'windows':
    DEFAULT_CONFIG_GROUP = 'windows'
    DEFAULT_COMMAND_LIST = os.path.join(SCRIPT_DIR, f'{SCRIPT_NAME}_commands.txt')
elif 'nunatak' in platform.node().lower():
    DEFAULT_CONFIG_GROUP = 'nunatak'
    DEFAULT_COMMAND_LIST = os.path.join(SCRIPT_DIR, f'{SCRIPT_NAME}_commands_nunatak.txt')
else:
    DEFAULT_CONFIG_GROUP = None
    DEFAULT_COMMAND_LIST = None
DEFAULT_ARGLIST = os.path.join(SCRIPT_DIR, f'{SCRIPT_NAME}_arglist.csv')
DEFAULT_SCRATCH_DIR = os.path.join(os.path.expanduser('~'), 'scratch', 'task_bundles')
PGC_ORTHO_SCRIPT = os.path.abspath(os.path.join(os.path.dirname(SCRIPT_DIR), 'pgc_ortho.py'))


class UnsupportedMethodError(Exception):
    def __init__(self, msg=""):
        super(Exception, self).__init__(msg)


def wrap_multiline_str(text, width=float('inf')):
    """Format a multiline string, preserving indicated line breaks.

    Wraps the `text` (a string) so every line is at most `width` characters long.
    Common leading whitespace from every line in `text` is removed.
    Literal '\n' are considered line breaks, and area treated as such in wrapping.

    Args:
        text (str): A multiline string to be wrapped.

    Returns:
        str: The wrapped string.

    Example:
        animal_a = "Cats"
        animal_b = "Dogs"
        text = wrap_multiline_rfstr(
            rf\"""
            Cats and dogs are the most popular pets in the world.
            \n  1) {animal_a} are more independent and are generally
            cheaper and less demanding pets.
            \n  2) {animal_b} are loyal and obedient but require more
            attention and exercise, including regular walks.
            \""", width=40
        )
        >>> print(text)
        Cats and dogs are the most popular pets
        in the world.
          1) Cats are more independent and are
        generally cheaper and less demanding
        pets.
          2) Dogs are loyal and obedient but
        require more attention and exercise,
        including regular walks.
    """
    s_in = textwrap.dedent(text.strip('\n'))

    p_in = [p for p in s_in.split(r'\n')]
    p_out = [textwrap.fill(p, width=width) for p in p_in]

    return '\n'.join(p_out)


class RawTextArgumentDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawTextHelpFormatter): pass

def get_config_value(config_dict, key, default_val=None):
    if key not in config_dict:
        val = default_val
    else:
        val = config_dict[key]
        if val == '':
            val = default_val
    return val

def get_arg_parser(
        config_file=None,
        config_group=DEFAULT_CONFIG_GROUP
):
    if config_file is None and DEFAULT_CONFIG_FILE is not None:
        if os.path.isfile(DEFAULT_CONFIG_FILE):
            config_file = DEFAULT_CONFIG_FILE

    command_list = None
    arglist = None
    srcdir = None
    dstdir = None
    dem = None
    dem_rootdir = None
    scratch = None

    provided_argstr_set = set()
    for token_idx, token in enumerate(sys.argv):
        if token.startswith('-'):
            provided_argstr_set.add(token.split('=')[0])
        if token.startswith('--config-file'):
            if token.startswith('--config-file='):
                config_file = token.split('=')[0]
            else:
                config_file = sys.argv[token_idx + 1]
        elif token.startswith('--config-group'):
            if token.startswith('--config-group='):
                config_group = token.split('=')[0]
            else:
                config_group = sys.argv[token_idx + 1]

    if '--config-on' in provided_argstr_set:
        use_config = True
    elif '--config-off' in provided_argstr_set:
        use_config = False
    else:
        use_config = True

    config_was_used = False
    if use_config and config_file is not None:
        config = configparser.ConfigParser()
        config.read(config_file)

        if config_group is None or config_group not in config:
            config_group = 'DEFAULT'
        if config_group in config:
            config_dict = config[config_group]
            config_was_used = True

            command_list = get_config_value(config_dict, 'command-list')
            arglist = get_config_value(config_dict, 'arglist')
            srcdir = get_config_value(config_dict, 'srcdir')
            dstdir = get_config_value(config_dict, 'dstdir')
            dem = get_config_value(config_dict, 'dem')
            dem_rootdir = get_config_value(config_dict, 'dem-rootdir')
            scratch = get_config_value(config_dict, 'scratch')

    if command_list is None:
        command_list = DEFAULT_COMMAND_LIST
    if arglist is None:
        arglist = DEFAULT_ARGLIST
    if scratch is None:
        scratch = DEFAULT_SCRATCH_DIR

    parser = argparse.ArgumentParser(
        formatter_class=RawTextArgumentDefaultsHelpFormatter,
        description=wrap_multiline_str("""
            Run a batch of pgc_ortho.py test commands.
        """)
    )

    if config_was_used:
        config_group_note = r'\n' + wrap_multiline_str(rf"""
            >>> Note that the config group '{config_group}' was used to populate
            job argument default settings <<<
        """)
    else:
        config_group_note = ''
    parser.add_argument(
        '--config-file',
        type=str,
        default=(config_file if config_file is not None else DEFAULT_CONFIG_FILE),
        help=wrap_multiline_str(rf"""
            Path to configuration file used to populate default script argument options.
            {config_group_note}
            \nProvide the --config-off option to disable automatic use of the
            default config file.
        """)
    )
    parser.add_argument(
        '--config-group',
        type=str,
        default=(config_group if config_group is not None else DEFAULT_CONFIG_GROUP),
        help=wrap_multiline_str(rf"""
            Group in --config-file to use for populating default script argument options.
        """)
    )
    parser.add_argument(
        '--config-off',
        action='store_true',
        help=wrap_multiline_str("""
            Do not use the --config-file settings, leaving script settings to be
            set from the provided arguments.
        """)
    )
    parser.add_argument(
        '--config-on',
        action='store_true',
        help=wrap_multiline_str("""
            Force useage of the --config-file settings in situations where
            the default config settings would not be automatically used.
        """)
    )

    parser.add_argument(
        '--command-list',
        type=str,
        default=command_list,
        help="Path to text file listing pgc_ortho.py commands to run."
    )

    parser.add_argument(
        '--arglist',
        type=str,
        default=arglist,
        help=wrap_multiline_str(r"""
            CSV file listing source image and corresponding DEM paths
            to pass as 'src' argument for pgc_ortho.py calls.
            \nInstances of '<ARGLIST>' in --command-list are replaced
            with this path.
            \nInstances of '<SRCDIR>' in this file are replaced with
            the --srcdir path.
            \nInstances of '<DEM_ROOTDIR>' in this file are replaced
            with the --dem-rootdir path.
            \n
        """)
    )

    parser.add_argument(
        '--srcdir',
        type=str,
        default=srcdir,
        help=wrap_multiline_str("""
            Root directory path where source imagery for tests lives.
            If this argument is provided, a copy of the --arglist
            file is made in the --scratch directory with the appropriate
            path substitutions made.
        """)
    )

    parser.add_argument(
        '--dstdir',
        type=str,
        default=dstdir,
        help=wrap_multiline_str(r"""
            Root directory path where output ortho imagery from tests
            will be created.
            \nInstances of '<DSTDIR>' in --command-list are replaced
            with this path.
            \n
        """)
    )

    parser.add_argument(
        '--dem',
        type=str,
        default=dem,
        help=wrap_multiline_str(r"""
            Path to single DEM (can be in VRT format handled by CSV arglist)
            used for all pgc_ortho.py calls.
            \nInstances of '<DEM>' in --command-list are replaced with
            this path.
            \n
        """)
    )

    parser.add_argument(
        '--dem-rootdir',
        type=str,
        default=dem_rootdir,
        help=wrap_multiline_str("""
            Root directory path where tiled DEM files referenced in
            --arglist CSV file live.
            If this argument is provided, a copy of the --arglist
            file is made in the --scratch directory with the appropriate
            path substitutions made.
        """)
    )

    parser.add_argument(
        '--scratch',
        type=str,
        default=scratch,
        help=wrap_multiline_str("""
            Directory where a copy of the --arglist file may be created.
        """)
    )

    parser.add_argument(
        '--dryrun',
        action='store_true',
        help="Print actions without exeuting."
    )

    return parser, provided_argstr_set


def main():
    # Set up console logging handler
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    lhandler = logging.StreamHandler()
    lhandler.setLevel(logging.DEBUG)
    lformatter = logging.Formatter('%(asctime)s %(levelname)s- %(message)s', '%m-%d-%Y %H:%M:%S')
    lhandler.setFormatter(lformatter)
    logger.addHandler(lhandler)

    parser, provided_argstr_set = get_arg_parser()
    script_args = parser.parse_args()

    if not script_args.command_list:
        parser.error("--command-list argument must be provided")
    if not os.path.isfile(script_args.command_list):
        parser.error("--command-list file does not exist: {}".format(script_args.command_list))
    script_args.command_list = os.path.abspath(script_args.command_list)

    if script_args.arglist is not None:
        if not os.path.isfile(script_args.arglist) and DEFAULT_CONFIG_GROUP != 'nunatak':
            logger.warning("--arglist file does not exist: {}".format(script_args.arglist))
        script_args.arglist = os.path.abspath(script_args.arglist)
    if script_args.srcdir is not None:
        if not os.path.isdir(script_args.srcdir):
            logger.warning("--srcdir directory does not exist: {}".format(script_args.srcdir))
        script_args.srcdir = os.path.abspath(script_args.srcdir)
    if script_args.dstdir is not None:
        script_args.dstdir = (
            script_args.dstdir
            .replace('<HOSTNAME>', platform.node())
            .replace('<USERNAME>', getpass.getuser())
        )
        if not os.path.isdir(script_args.dstdir):
            logger.warning("--dstdir directory does not exist: {}".format(script_args.dstdir))
            logger.info("--dstdir directory will be created")
        script_args.dstdir = os.path.abspath(script_args.dstdir)
    if script_args.dem is not None:
        if not os.path.isfile(script_args.dem):
            logger.warning("--dem file does not exist: {}".format(script_args.dem))
        script_args.dem = os.path.abspath(script_args.dem)
    if script_args.dem_rootdir is not None:
        if not os.path.isdir(script_args.dem_rootdir):
            logger.warning("--dem-rootdir directory does not exist: {}".format(script_args.dem_rootdir))
        script_args.dem_rootdir = os.path.abspath(script_args.dem_rootdir)
    if script_args.scratch is not None:
        if not os.path.isdir(script_args.scratch):
            logger.warning("--scratch directory does not exist: {}".format(script_args.scratch))
        script_args.scratch = os.path.abspath(script_args.scratch)

    if os.path.isfile(script_args.arglist):
        if not os.path.isdir(script_args.scratch):
            logger.warning("Cannot make copy of --arglist file because --scratch directory does not exist")
            logger.warning("If arglist requires string replacements, tasks will fail")
        else:
            arglist_copy = os.path.join(script_args.scratch, os.path.basename(script_args.arglist))
            logger.info("Making copy of arglist for this run: {}".format(arglist_copy))
            if True or (not script_args.dryrun):
                with open(arglist_copy, 'w') as arglist_copy_fp:
                    with open(script_args.arglist, 'r') as arglist_orig_fp:
                        for line in arglist_orig_fp:
                            arglist_copy_fp.write(
                                line
                                .replace('<SRCDIR>', str(script_args.srcdir))
                                .replace('<DEM>', str(script_args.dem))
                                .replace('<DEM_ROOTDIR>', str(script_args.dem_rootdir))
                            )
            script_args.arglist = arglist_copy

    if script_args.dstdir:
        if not os.path.isdir(script_args.dstdir):
            logger.info("Creating --dstdir directory: {}".format(script_args.dstdir))
            if not script_args.dryrun:
                os.makedirs(script_args.dstdir)
        logger.info("Creating output test folders within dstdir: {}".format(script_args.dstdir))
        if not script_args.dryrun:
            for test_num in range(1, 11):
                test_dir = os.path.join(script_args.dstdir, f"test_{test_num:02d}")
                os.makedirs(test_dir, exist_ok=True)

    logger.info("Running test commands from command list file: {}".format(script_args.command_list))

    with open(script_args.command_list, 'r') as command_list_fp:
        test_num = 0
        for line in command_list_fp:
            if line.strip() == '':
                continue
            else:
                cmd = line.strip()
                test_num += 1
            cmd = (
                cmd
                .replace('<PGC_ORTHO_SCRIPT_LOCATION>', PGC_ORTHO_SCRIPT)
                .replace('<ARGLIST>', str(script_args.arglist))
                .replace('<SRCDIR>', str(script_args.srcdir))
                .replace('<DSTDIR>', str(script_args.dstdir))
                .replace('<DEM>', str(script_args.dem))
                .replace('<DEM_ROOTDIR>', str(script_args.dem_rootdir))
            )
            logger.info(f"Running TEST {test_num:02d} with the following command:")
            logger.info(f"{cmd}")
            if not script_args.dryrun:
                subprocess.call(cmd, shell=True)

    logger.info("Test runs are complete")


if __name__ == '__main__':
    main()

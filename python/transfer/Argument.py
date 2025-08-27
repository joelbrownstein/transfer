from argparse import ArgumentParser

class Argument:
    
    def __init__(self, name=None):
        self.get_options = globals()[name] if name in globals().keys() else None
        self.program, self.options = self.get_options() if self.get_options else (None, None)

def transfer():
    parser = ArgumentParser()
    parser.add_argument('-m', '--mjd', action='store', dest='mjd', type=int, metavar='MJD', help='Transfer this MJD')
    parser.add_argument('-I', '--ini_mode', action='store', dest='ini_mode', metavar='INI_MODE', help='ini mode', choices=['mos','lvm'])
    parser.add_argument('-L', '--log_dir', action='store', dest='log_dir', metavar='LOG_DIR', help='ini mode')
    section = parser.add_mutually_exclusive_group()
    section.add_argument('-i', '--include', action='append', dest='include', metavar='SECTION', help='Include this')
    section.add_argument('-e', '--exclude', action='append', dest='exclude', metavar='SECTION', help='Exclude this')
    parser.add_argument('-R', '--report', action='store_true', dest='report', help='Download Report')
    parser.add_argument('-D', '--download', action='store_true', dest='download', help='Download Data')
    parser.add_argument('-V', '--verify', action='store_true', dest='verify', help='Verify Data')
    parser.add_argument('-B', '--backup', action='store_true', dest='backup', help='Backup to HPSS')
    parser.add_argument('-O', '--observatory', action='store', dest='observatory', metavar='OBSERVATORY', help='observatory', choices = ['apo','lco'])
    parser.add_argument('-C', '--copy', action='store_true', dest='copy', help='Copy to SAS')
    parser.add_argument('-M', '--mirror', action='store_true', dest='mirror', help='Mirror to SAM')
    parser.add_argument('-S', '--sync', action='store', dest='sync', metavar='SYNC', help='sync', choices=['init','final'])
    parser.add_argument('-d', '--debug', action='store_true', dest='debug', help='Set logger to debug')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose', help='Set verbose')
    args = parser.parse_args()
    if not (args.report or args.download or args.verify or args.backup or args.copy or args.mirror):
        args.report = True
        args.download = True
        args.verify = True
        args.backup = True
        args.copy = True
        args.mirror = True
    return parser.prog, args

def transfer_github():
    parser = ArgumentParser()
    parser.add_argument("-b", "--branch", help="set branch",metavar="BRANCH")
    parser.add_argument("-p", "--product", help="set product",metavar="PRODUCT")
    parser.add_argument("-d", "--days", help="set days ago",type=int,metavar="DAYS")
    parser.add_argument("-k", "--key", help="github key",metavar="KEY")
    parser.add_argument("-v", "--verbose", help="verbose",action="store_true")
    args = parser.parse_args()
    return parser.prog, args

def transfer_mirror():
    parser = ArgumentParser()
    parser.add_argument("-l", "--location", help="location",metavar="LOCATION")
    parser.add_argument("-d", "--dryrun", help="dryrun",action="store_true")
    parser.add_argument("-v", "--verbose", help="verbose",action="store_true")
    args = parser.parse_args()
    return parser.prog, args

def transfer_rclone():
    parser = ArgumentParser()
    parser.add_argument('-e', '--env', action='store', dest='env', metavar='ENV', help='Rclone this Env')
    parser.add_argument('-o', '--observatory', action='store', dest='observatory', metavar='OBSERVATORY', help='Observatory')
    parser.add_argument('-m', '--mjd', action='store', dest='mjd', type=int, metavar='MJD', help='Rclone this MJD')
    parser.add_argument('-d', '--dir', action='store', dest='dir', metavar='DIR', help='Log to dir')
    parser.add_argument("-D", "--dryrun", help="dryrun",action="store_true")
    parser.add_argument("-v", "--verbose", help="verbose",action="store_true")
    args = parser.parse_args()
    return parser.prog, args

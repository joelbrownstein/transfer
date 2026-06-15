from os import chdir, makedirs, environ, listdir
from os.path import join, exists
from json import loads
from urllib.request import urlopen
from time import sleep
from shutil import copyfile
import tarfile
from collections import OrderedDict
from zstandard import ZstdCompressor

class Backup:

    servers = ('archive')
    crc = '-H server=archive.nersc.gov:crc:verify=all'
    perm = 0o775

    def __init__(self, staging=None, observatory=None, mode=None, mjd=None, process=None, dir=None, logger=None, server=None, stage=None, verbose=None):
        self.staging = staging
        self.mjd = mjd
        self.process = process
        self.dir = dir
        self.logger = logger
        stage = stage if stage else 'backup'
        self.stage_backup = ( stage == 'backup' )
        self.stage_mirror = ( stage == 'mirror' )
        self.verbose = verbose
        self.set_server(server=server)
        observatory_mode = observatory if mode=='mos' else mode
        self.set_stage(observatory=observatory_mode)
        self.set_mjd_dir(observatory=observatory_mode)
        self.set_hpss_staging_dir(observatory=observatory_mode)
        self.set_cloud_staging_dir(observatory=observatory_mode)
        self.set_dir()
        self.tarfiles = OrderedDict()
        self.ready = True
        if self.verbose: print("BACKUP> ready=%r" % self.ready)

    def set_stage(self, observatory=None):
        self.stage = "transfer.%s.backup" % observatory if observatory else "transfer.backup"
    
    def set_mjd_dir(self, observatory=None):
        try: self.dir = environ["TRANSFER_BACKUP_DIR"]
        except: self.dir = None
        system = self.server['system'] if self.server else None
        location = join(observatory, system) if observatory and system else None
        self.mjd_dir = join(self.dir, location, str(self.mjd)) if self.dir and location and self.mjd else None
        if self.mjd_dir and not exists(self.mjd_dir):
            try:
                makedirs(self.mjd_dir, self.perm)
                if self.verbose: print("BACKUP> CREATE: %r" % self.mjd_dir)
            except Exception as e:
                print("BACKUP> %r" % e)
                self.mjd_dir = None

    def set_hpss_staging_dir(self, observatory = None):
        self.hpss_staging_dir = join(self.dir, 'hpss', 'staging', observatory)
        
    def set_cloud_staging_dir(self, observatory = None):
        self.cloud_staging_dir = join(self.dir, 'cloud', 'staging', observatory)
        
    def set_dir(self):
        if self.mjd_dir:
            if exists(self.mjd_dir):
                if self.stage_backup:
                    ls = listdir(self.mjd_dir)
                    n = max([int(d) for d in ls]) + 1 if len(ls) > 0 else 0
                    self.dir = join(self.mjd_dir, str(n))
                    try:
                        makedirs(self.dir, self.perm)
                        if self.verbose: print("BACKUP> CREATE: %r" % self.dir)
                    except Exception as e:
                        print("BACKUP> %r" % e)
                        self.dir = None
                elif self.stage_mirror:
                    self.dir = self.mjd_dir
                    if self.verbose: print("BACKUP> MIRROR to %r" % self.dir)
                else:
                    if self.verbose: print("BACKUP> Invalid stage=%r" % self.stage)
                    self.dir = None
            else:
                if self.verbose: print("BACKUP> Nonexistent MJD dir %r" % self.mjd_dir)
                self.dir = None
        else:
            if self.verbose: print("BACKUP> NULL MJD dir %r" % self.mjd_dir)
            self.dir = None


    def set_tar_dir(self):
        if self.stage_backup:
            self.tar_dir = join(self.dir, self.section)
            self.process.mkdir(self.tar_dir, silent=True)
        elif self.stage_mirror: self.tar_dir = self.dir
        else: self.tar_dir = None
        
    def set_server(self, server=None):
        if not server:
            try: server = environ['TRANSFER_BACKUP_SERVER']
            except: pass
        self.server = {'system': server if server in self.servers else self.servers[0]}
        self.server['url'] =  "https://newt.nersc.gov/newt/status/%(system)s" % self.server
    
    def set_section_dir(self):
        if self.staging and self.section:
            boss_section = self.section in ['sos', 'spectro'] if self.section else None
            folder = join('boss',self.section) if boss_section else self.section
            self.section_dir = join(self.staging,folder) if folder else None
            if self.section_dir and exists(self.section_dir): chdir(self.section_dir)
            else:
                if self.verbose: print("BACKUP> Nonexistent section dir %r" % self.section_dir)
                self.ready = False
        else: self.ready = False

    def set_tarfile(self):
        self.tarfile = {}
        if self.dir:
            ext = "tgz" if self.stage_mirror else "tar"
            self.tarfile['file'] = file = "{mjd}_{section}.{ext}".format(mjd=self.mjd, section=self.section, ext=ext)
            self.tarfile['local'] =  join(self.tar_dir, file)
            self.tarfile['hpss-staging'] =  join(self.hpss_staging_dir, self.section, file) if self.hpss_staging_dir else None
            self.tarfile['cloud-staging'] =  join(self.cloud_staging_dir, self.section, file) if self.cloud_staging_dir else None

    def tar(self):
        self.set_section_dir()
        self.set_tar_dir()
        if self.ready:
            self.set_tarfile()
            if self.tarfile and exists(str(self.mjd)):
                force = True
                if exists(self.tarfile['local']) or force==True:
                    filemode = "w"
                    if self.stage_mirror: filemode += ":gz"
                    with tarfile.open(self.tarfile['local'], filemode) as tar: tar.add(str(self.mjd))
                    self.tarfiles[self.section] = self.tarfile
                    self.logger.info("tar create %(local)s" % self.tarfile)
                    if self.verbose: print("BACKUP> tar %(local)s" % self.tarfile)
                else:
                    self.logger.warning("Found %r [skip] %r" % self.tarfile)
                    print("Found %r [skip]" % self.tarfile)
            else: self.logger.warning("Skipping %r" % self.tarfile)
            
    def copy_to_hpss_staging(self):
        source = self.tarfile['local']
        destination = self.tarfile['hpss-staging']
        try:
            if exists(source):
                copyfile(source, destination)
                self.logger.warning("HPSS STAGING> %(hpss-staging)s" % self.tarfile)
                if self.verbose: print("HPSS STAGING> %(hpss-staging)s" % self.tarfile)
            else:
                self.logger.warning("HPSS STAGING> Non-existent %(local)s" % self.tarfile)
                print("HPSS STAGING> Missing path=%(local)r" % self.tarfile)
        except Exception as e:
            self.logger.warning("HPSS STAGING> Failed to copy: %r" % e)
            print("HPSS STAGING> Failed to copy: %r" % e)
        
    def zstd_to_cloud_staging(self):
        source = self.tarfile['local']
        destination = self.tarfile['cloud-staging']
        if exists(source):
            threads = 12
            chunk_size = 32 * 1024 * 1024  
            try:
                zstd_compressor = ZstdCompressor(level=9, threads=threads)
                with open(source, 'rb') as tarball:
                    with open(destination, 'wb') as file:
                        with zstd_compressor.stream_writer(file) as compressor:
                            while chunk := tarball.read(chunk_size):
                                compressor.write(chunk)
                self.logger.warning("CLOUD STAGING> %(cloud-staging)s" % self.tarfile)
                if self.verbose: print("CLOUD STAGING> %(cloud-staging)s" % self.tarfile)
            except Exception as e:
                self.logger.warning("CLOUD STAGING> Zstandard compression failed: %r" % e)
                print("CLOUD STAGING> Failed to compress: %r" % e)
        else:
            self.logger.warning("CLOUD STAGING> Non-existent %(local)s" % self.tarfile)
            print("CLOUD STAGING> Missing path=%(local)r" % self.tarfile)

from os import chdir, makedirs, environ, listdir
from os.path import join, exists
from json import loads
from urllib.request import urlopen
from time import sleep
from shutil import copyfile
import tarfile
from transfer import Remote
from transfer import Globus
from collections import OrderedDict

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
        self.set_dir()
        self.set_remote_dir(staging=staging)
        self.tarfiles = OrderedDict()
        self.globus_transfer = None
        # no globus
        self.globus = Globus(staging=staging, observatory=observatory, mode=mode, mjd=mjd, hpss=True,  process=process, dir=dir, scratch_dir=self.dir, logger=logger, verbose=verbose) if self.stage_backup else None
        # with globus
        #self.globus = Globus(staging=staging, observatory=observatory, mjd=mjd, hpss=self.stage_backup, sam=self.stage_mirror, process=process, dir=dir, scratch_dir=self.dir, logger=logger, verbose=verbose)
        if self.stage_backup:
            if self.dir and self.remote_dir and self.server and self.process and self.process.ready: self.set_ready()
        elif self.stage_mirror:
            self.ready = True if self.dir else False
        else: self.ready = False
        if self.verbose: print("BACKUP> ready=%r" % self.ready)

    def set_stage(self, observatory=None):
        self.stage = "transfer.%s.backup" % observatory if observatory else "transfer.backup"

    def set_remote(self):
        if self.stage_backup:
            try: username, hostname = environ['TRANSFER_BACKUP_USER'], environ['TRANSFER_BACKUP_HOST']
            except: username, hostname = (None, None)
            self.remote = Remote(username=username, hostname=hostname, verbose=self.verbose) if username and hostname else None
        else: set.remote = None
    
    def set_mjd_dir(self, observatory=None):
        env =  "TRANSFER_BACKUP_DIR" if self.stage_backup else "TRANSFER_MIRROR_BACKUP" if self.stage_mirror else None
        try: self.dir = environ[env]
        except: self.dir = None
        system = self.server['system'] if self.server else None
        
        location = ( join(observatory, system) if self.stage_backup else join(system, observatory) if self.stage_mirror else None ) if observatory and system else None
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
        

    def set_remote_output(self, command=None):
        self.remote.set_stdout(file = self.get_remote_file(command=command, ext='out.txt'))
        self.remote.set_stderr(file = self.get_remote_file(command=command, ext='err.txt'))

    def set_server(self, server=None):
        if not server:
            try: server = environ['TRANSFER_BACKUP_SERVER']
            except: pass
        self.server = {'system': server if server in self.servers else self.servers[0]}
        self.server['url'] =  "https://newt.nersc.gov/newt/status/%(system)s" % self.server
    
    def set_remote_dir(self, staging=None):
        if self.stage_backup:
            try: self.remote_dir = staging.replace(environ['SAS_ROOT'],environ['HPSS_BASE_DIR'])##fix
            except: self.remote_dir = None
            if self.verbose: print("BACKUP> HPSS remote_dir=%r" % self.remote_dir)
        else: self.remote_dir = None
    
    def set_ready(self, count=1, limit=10, seconds=600):
        try: self.ready = bool(eval(environ['TRANSFER_BACKUP_READY']))
        except: self.ready = False
        if not self.ready:
            self.ready = self.globus and self.globus.ready
            if self.ready:
                try:
                    self.logger.debug("Checking %(url)s." % self.server)
                    self.server.update(loads(urlopen(self.server['url']).read().decode()))
                    self.ready = True if self.server['status'] == 'up' else False
                    self.logger.warn("HPSS server %(system)s is %(status)s." % self.server)
                except: pass
                if not self.ready:
                    if count <= limit:
                        sleep(seconds)
                        self.logger.warn("Waiting for HPSS to come up.  Retrying [%r/%r]." % (count, limit))
                        self.set_ready(count=count+1)
                    else: self.logger.critical("HPSS not up after %r tries.  Giving up!" % limit)
            else: self.logger.critical("Globus not connected.  Giving up!")

    def set_remote_path(self):
        self.remote_path = join(self.remote_dir, self.section, '') if self.remote_dir and self.section else None

    def mkdir_remote_path(self):
        if self.ready and self.remote and self.remote.connected:
            if self.remote_path:
                self.set_remote_output(command='hsi_mkdir')
                #command = "hsi -s archive mkdir -m 2750 -p " + self.remote_path
                command = "/usr/common/mss/bin/hsi mkdir -m 2750 -p " + self.remote_path
                self.logger.debug(command)
                #self.process.run(command)
                self.remote.exec_command(command)
                if self.remote.return_code:
                    self.ready = False
                    self.logger.critical("HSI return code %r. Giving up!" % self.remote.return_code)
            else:
                self.ready = False
                self.logger.critical("HSI mkdir requires valid path. Giving up!")

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

    def get_remote_file(self, command=None, ext=None):
        command = command if command else "proc"
        file = "transfer.remote.{hostname}.{command}.{ext}" if ext else "transfer.remote.{hostname}.{command}"
        file = file.format(hostname=self.remote.hostname, command=command, ext=ext)
        return join(self.tar_dir, file) if self.tar_dir else file


    def set_tarfile(self):
        self.tarfile = {}
        if self.dir:
            ext = "tgz" if self.stage_mirror else "tar"
            self.tarfile['file'] = file = "{mjd}_{section}.{ext}".format(mjd=self.mjd, section=self.section, ext=ext)
            self.tarfile['local'] =  join(self.tar_dir, file)
            self.tarfile['hpss-staging'] =  join(self.hpss_staging_dir, self.section, file) if self.hpss_staging_dir else None
            self.tarfile['remote'] =  join(self.remote_dir, self.section, file) if self.remote_dir else None

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
                self.logger.warning("BACKUP STAGING> %(hpss-staging)s" % self.tarfile)
                if self.verbose: print("BACKUP STAGING> %(hpss-staging)s" % self.tarfile)
            else:
                self.logger.warning("BACKUP STAGING> Non-existent %(local)s" % self.tarfile)
                print("BACKUP STAGING> %(hpss-staging)s" % self.tarfile)
        except Exception as e:
            self.logger.warning("BACKUP STAGING> Failed to copy: %r" % e)
            print("BACKUP STAGING> Failed to copy: %r" % e)
        
    def set_globus_transfer(self):
        if self.ready and self.globus.ready:
            self.globus.set_options(sync = 'mtime', preserve_mtime = True, verify = True)
            for self.globus.section, tarfile in self.tarfiles.items():
                self.globus.append_target_for_backup(tarfile=tarfile)
            self.globus.commit()

    def globus_submit(self):
        if self.ready and self.globus.ready:
            self.globus.submit()
            self.globus.wait()
            self.globus.set_details()
            self.globus.set_status()
            self.globus.write_logfile()

    """def set_globus_transfer(self):
        if self.ready:
            self.globus_transfer = Globus_Transfer(configfile="%s.ini" % self.stage, mjd=self.mjd)
            self.globus_transfer.outfile = join(self.dir, "%s.globus.cli" % self.stage)
            self.globus_transfer.logfile = join(self.dir, "%s.globus.log" % self.stage)
            self.globus_transfer.errfile = join(self.dir, "%s.globus.err" % self.stage)
            self.globus_transfer.commit()
            if self.globus_transfer.endpoint.config.file:
                self.globus_transfer.endpoint.config.source['dir'] = self.dir
                self.globus_transfer.endpoint.config.destination['dir'] = self.remote_dir
            else:
                self.globus_transfer = None
                self.ready = False
        else: self.globus_transfer = None


    def globus_submit(self):
        print("BACKUP> globus_submit globus_transfer=%r" % self.globus_transfer )
        if self.ready and self.globus_transfer:
            print("transfer %r" % self.tarfiles )
            self.logger.debug("transfer %r" % self.tarfiles)
            item0 = self.globus_transfer.endpoint.config.item[0]
            self.globus_transfer.endpoint.config.item = []
            for section, tarfile in self.tarfiles.items():
                item = item0.copy()
                item['dir'] = section
                item['file'] = tarfile['file']
                self.globus_transfer.endpoint.config.item.append(item)
            #self.globus_transfer.submit()"""



    def htar_idx(self):
        if self.ready:
            self.set_tar_dir()
            self.set_remote_output(command='htar_idx')
            command="/usr/common/mss/bin/htar -Xf %(remote)s" % self.tarfile
            self.remote.exec_command(command)
            self.logger.info("htar -Xf %(remote)s" % self.tarfile)

    """def htar(self, count=1, limit=5, seconds=60):
        self.set_section_dir()
        if self.ready:
            command = "htar -cvhf {0}/{1}/{2}_{1}.tar {3} {2}".format(self.remote_dir, self.section, self.mjd, self.crc)
            self.logger.debug(command)
            self.process.run(command)
            self.success = True if self.process.out.find('HTAR: HTAR SUCCESSFUL') > -1 else False
            if self.success: self.logger.warn("HTAR success on section %s " % self.section)
            else:
                if count <= limit:
                    sleep(seconds)
                    self.logger.warn("HTAR Failed on section %s. Retrying[%r/%r]." % (self.section, count, limit))
                    self.backup(count=count+1)
                else:
                    self.ready = False
                    self.logger.critical("HTAR failed on section %s after %r tries. Giving up!" % (self.section,limit))"""
            

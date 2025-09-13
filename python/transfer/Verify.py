from transfer import Config, Process, Logging
from os import chdir, getcwd, listdir, environ, rmdir
from os.path import join, exists, isdir, basename
import re
import gzip

class Verify:

    def __init__(self, options=None, staging=None, observatory=None, mode=None, mjd=None, process=None, dir=None, logger=None, stage = None, debug = None, verbose=None):
        self.options = options
        self.staging = staging
        self.observatory = observatory
        self.mode = mode
        self.mjd = mjd
        self.process = process
        self.dir = dir
        self.logger = logger
        self.stage = stage
        self.debug = debug
        self.verbose = verbose
        self.sumfile = None
        self.ready = None
    
    def set_section(self, section = None):
        if section:
            self.ready = True
            boss_section = section in ['sos', 'spectro'] if section else None
            folder = join('boss',section) if boss_section else section
            if self.stage == 'verify':
                self.mjd_dir = join(self.staging,folder,str(self.mjd))
            elif self.stage == 'reverify':
                env = self.options.get(section,'env_copy')
                try: sas_dir = environ[env] if env else None
                except: sas_dir = None
                self.mjd_dir = join(sas_dir,str(self.mjd)) if sas_dir else None
            else: self.mjd_dir = None
            self.mjd_dir_nonempty = True if self.mjd_dir and isdir(self.mjd_dir) and listdir(self.mjd_dir) else False
            method = self.options.get(section,'verify')
            if not self.debug:
                if method != 'SKIP' and self.mjd_dir_nonempty:
                    self.sumfile = join(self.mjd_dir,'irsc.log.gz') if method == 'ircam' else join(self.mjd_dir,"{0:d}.{1}".format(self.mjd,method.split(' ')[0]))
                    if self.verbose: print("TRANSFER> Verify %s using sumfile=%r" % (section, self.sumfile))
                    if exists(self.sumfile):
                        self.logger.info("{0} file exists, running {1} verification stage.".format(self.sumfile,section))
                        if method == 'ircam':
                            cRre = re.compile(r'(cR\d{6}\.fit)(\.gz|)\s*')
                            with gzip.open(self.sumfile, "rt") as f: lines = f.read()
                            ircamlog = dict()
                            for l in lines.split('\n'):
                                if len(l) > 0:
                                    m = cRre.match(l)
                                    if m is None: continue
                                    else:
                                        k = m.groups()[0]
                                        try:
                                            ircamlog[k].append(l)
                                        except KeyError:
                                            ircamlog[k] = [ l ]
                            sortedloglist = list(ircamlog.keys())
                            sortedloglist.sort()
                            sorteddisklist = list()
                            for d in listdir(self.mjd_dir):
                                m = cRre.match(d)
                                if m is not None: sorteddisklist.append(m.groups()[0])
                            sorteddisklist.sort()
                            if len(sortedloglist) == len(sorteddisklist):
                                self.logger.info("Number of files in irsc.log equals number of files on disk (%r)" % len(sortedloglist))
                                for k in range(len(sorteddisklist)):
                                    if sorteddisklist[k] != sortedloglist[k]:
                                        self.logger.error("WARNING: file #{0}: {1} {2}!".format(k,sorteddisklist[k],sortedloglist[k]))
                                        self.ready = False
                            else:
                                if len(sortedloglist) > len(sorteddisklist):
                                    self.logger.error("Number of files in irsc.log exceeds number of files on disk (%r>%r)" % (len(sortedloglist),len(sorteddisklist)))
                                    for file in sortedloglist:
                                        if file not in sorteddisklist: self.logger.error("    --> Missing %s on disk" % file)
                                    for file in sorteddisklist:
                                        if file not in sortedloglist: self.logger.error("    --> And missing %s in irsc.log" % file)
                                if len(sortedloglist) < len(sorteddisklist):
                                    self.logger.error("Fewer files in irsc.log than the number of files on disk (%r<%r)" % (len(sortedloglist),len(sorteddisklist)))
                                    for file in sorteddisklist:
                                        if file not in sortedloglist: self.logger.error("    --> Missing %s in irsc.log" % file)
                                    for file in sortedloglist:
                                        if file not in sorteddisklist: self.logger.error("    --> And Missing %s on disk" % file)
                                self.ready = False

                        else:
                            oldwd = getcwd()
                            chdir(self.mjd_dir)
                            command = "{0} {1}".format(method,self.sumfile)
                            self.process.run(command)
                            for c in self.process.out.split("\n"):
                                if len(c) > 0:
                                    l = c.rsplit(':',1)
                                    try: foo = l[1].index('OK')
                                    except ValueError:
                                        self.logger.error("Checksum mismatch: {0}".format(l[0]))
                                        self.ready = False
                            chdir(oldwd)
                    else: self.ready = False
                elif not self.mjd_dir_nonempty: self.logger.info("No {0} data found.".format(section))


    """def run_reverify(self):
        if self.ready:
            self.logging.set_stage(stage=self.stage)
            logger = self.logging.logger
            options = self.config.options
            for section in self.sections:
                env = self.config.options.get(section,'env_copy')
                try: sas_dir = environ[env] if env else None
                except: sas_dir = None
                self.mjd_dir = join(sas_dir,str(self.mjd)) if sas_dir else None
                print("MJD DIR> %r" % self.mjd_dir)
                mjd_dir_nonempty = True if self.mjd_dir and isdir(self.mjd_dir) and listdir(self.mjd_dir) else False
                method = options.get(section,'verify')
                if not self.mjd_dir:
                    if self.verbose: print("REVERIFY> Please module load tree/sdsswork to set env=%r" % env)
                elif not self.debug:
                    if method != 'SKIP' and mjd_dir_nonempty:
                        sumfile = join(self.mjd_dir,'irsc.log.gz') if method == 'ircam' else join(self.mjd_dir,"{0:d}.{1}".format(self.mjd,method.split(' ')[0]))
                        if self.verbose: print("REVERIFY> Verify %s using sumfile=%r" % (section, sumfile))
                        if exists(sumfile):
                            logger.info("{0} file exists, running {1} verification stage.".format(sumfile,section))
                            if method == 'ircam':
                                cRre = re.compile(r'(cR\d{6}\.fit)(\.gz|)\s*')
                                with gzip.open(sumfile, "rt") as f: lines = f.read()
                                ircamlog = dict()
                                for l in lines.split('\n'):
                                    if len(l) > 0:
                                        m = cRre.match(l)
                                        if m is None: continue
                                        else:
                                            k = m.groups()[0]
                                            try:
                                                ircamlog[k].append(l)
                                            except KeyError:
                                                ircamlog[k] = [ l ]
                                sortedloglist = list(ircamlog.keys())
                                sortedloglist.sort()
                                sorteddisklist = list()
                                for d in listdir(self.mjd_dir):
                                    m = cRre.match(d)
                                    if m is not None: sorteddisklist.append(m.groups()[0])
                                sorteddisklist.sort()
                                if len(sortedloglist) == len(sorteddisklist):
                                    logger.info("Number of files in irsc.log equals number of files on disk (%r)" % len(sortedloglist))
                                    for k in range(len(sorteddisklist)):
                                        if sorteddisklist[k] != sortedloglist[k]:
                                            logger.error("WARNING: file #{0}: {1} {2}!".format(k,sorteddisklist[k],sortedloglist[k]))
                                            self.ready = False
                                else:
                                    if len(sortedloglist) > len(sorteddisklist):
                                        logger.error("Number of files in irsc.log exceeds number of files on disk (%r>%r)" % (len(sortedloglist),len(sorteddisklist)))
                                        for file in sortedloglist:
                                            if file not in sorteddisklist: logger.error("    --> Missing %s on disk" % file)
                                        for file in sorteddisklist:
                                            if file not in sortedloglist: logger.error("    --> And missing %s in irsc.log" % file)
                                    if len(sortedloglist) < len(sorteddisklist):
                                        logger.error("Fewer files in irsc.log than the number of files on disk (%r<%r)" % (len(sortedloglist),len(sorteddisklist)))
                                        for file in sorteddisklist:
                                            if file not in sortedloglist: logger.error("    --> Missing %s in irsc.log" % file)
                                        for file in sortedloglist:
                                            if file not in sorteddisklist: logger.error("    --> And Missing %s on disk" % file)
                                    self.ready = False

                            else:
                                oldwd = getcwd()
                                chdir(self.mjd_dir)
                                command = "{0} {1}".format(method,sumfile)
                                self.process.run(command)
                                for c in self.process.out.split("\n"):
                                    if len(c) > 0:
                                        l = c.rsplit(':',1)
                                        try: foo = l[1].index('OK')
                                        except ValueError:
                                            logger.error("Checksum mismatch: {0}".format(l[0]))
                                            self.ready = False
                                chdir(oldwd)
                        else:
                            logger.error("{0} does not appear to exist!".format(sumfile))
                            self.ready = False
                    elif not mjd_dir_nonempty: logger.info("No {0} data found.".format(section))
                if mjd_dir_nonempty:
                    if self.summary: self.summary.export_section(directory=self.mjd_dir, section=section)
                    logger.info("Export summary for section={0}.".format(section))

            if not self.debug:
                if self.ready:
                    if self.summary: self.summary.save(stage=self.stage, status='success')
                else:
                    if self.summary: self.summary.save(stage=self.stage, status='failure')
                    logger.critical("Errors verifying {0} data!".format(section))
    """

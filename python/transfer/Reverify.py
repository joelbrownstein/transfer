from transfer import Config, Process, Logging
from os import chdir, getcwd, listdir, environ, rmdir
from os.path import join, exists, isdir, basename
import re
import gzip

class Reverify:

    def __init__(self, options=None, observatory=None, mjd=None, ini_mode=None, log_dir=None, include=None, exclude=None, debug=False, verbose=False):
        self.observatory = options.observatory if options else observatory
        self.ini_mode = options.ini_mode if options else ini_mode
        self.log_dir = options.log_dir if options else log_dir
        self.verbose = options.verbose if options else verbose
        self.mjd = options.mjd if options and options.mjd else mjd
        self.include = options.include if options else include
        self.exclude = options.exclude if options else exclude
        self.debug = options.debug if options else debug
        self.ready = False
        self.stage = 'reverify'
    
    def set_config(self):
        self.config = Config(observatory = self.observatory,  log_dir = self.log_dir, ini_mode = self.ini_mode, verbose = self.verbose)
        if not self.mjd: self.mjd = self.config.current_mjd()
        if self.verbose: print("REVERIFY> MJD=%r" % self.mjd)

    def set_logging(self):  self.logging = Logging(staging = self.config.staging, observatory = self.config.observatory, log_dir = self.config.log_dir, mode = self.config.mode, mjd = self.mjd, debug = self.debug, verbose = self.verbose)

    def set_process(self, program=None):  self.process = Process(program = program, mjd = self.mjd, logger = self.logging.logger, verbose = self.verbose)

    def set_sections(self):
        self.sections = [section for section in self.config.options.sections() if section!='general']
        if self.include: self.sections = [section for section in self.sections if section in self.include]
        if self.exclude: self.sections = [section for section in self.sections if section not in self.exclude]
        if self.verbose: print("REVERIFY> Sections=%r" % self.sections)
        self.ready = True if self.sections and self.logging.ready and self.process.ready else False
    
    def set_history(self, mode=None, status=None):
        self.history = History()
        """self.summary = Summary(staging = self.config.staging, observatory = self.config.observatory, log_dir=self.config.log_dir, mjd = self.mjd, logfile=self.current_report, verbose = self.verbose)
        if status: self.summary.todo_status = status
        for stage in self.summary.stages.keys(): self.summary.stages[stage] = getattr(self,stage)
        self.logging.logger.info("Ready to run stages [%s]" % ', '.join(self.summary.stages_todo()))
        if not self.debug: self.summary.save(stage = self.stage)"""

    def run_verify(self):
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

    def done(self):
        self.logging.set_stage()
        self.logging.logger.info("Done!")

Class History:

    def __init__(self, staging=None, observatory=None, log_dir = None, mjd=None, logfile=None, verbose=False):
        self.staging = staging
        self.generation = 5 if 'data' in staging else 4
        self.observatory = observatory
        self.log_dir = log_dir if log_dir else 'atlogs'
        self.mjd = mjd
        self.logfile = logfile
        self.verbose = verbose

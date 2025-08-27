from os import makedirs, environ, listdir
from os.path import join, exists
import logging
from logging.handlers import RotatingFileHandler, SMTPHandler
from socket import getfqdn

class Logging:

    perm = 0o775
    time_format = "%Y-%m-%dT%H:%M:%S %Z"

    def __init__(self, staging=None, observatory=None, mjd=None, log_dir = None, dir = None, mode = None, debug=False, verbose=False):
        if verbose: print("LOGGER> staging: %r, observatory: %r log_dir: %r, mode: %r" % (staging, observatory, log_dir, mode))
        self.staging = staging
        self.observatory = observatory
        self.mjd = mjd
        self.log_dir = log_dir
        self.mode = mode
        self.debug = debug
        self.verbose = verbose
        self.logger = None
        self.filehandler = None
        self.smtphandler = None
        self.set_mjd_dir()
        self.set_dir(dir = dir)
        self.set_mailhost()
        self.set_recipients()
        self.set_email()
        self.set_file()
        self.set_stage()
    
    def set_mailhost(self):
        try: self.mailhost = environ['TRANSFER_MAILHOST']
        except: self.mailhost = None
    
    def set_recipients(self):
        try: self.recipients = [recipient.strip() for recipient in environ['TRANSFER_RECIPIENTS'].split(',')]
        except: self.recipients = None
    
    def set_email(self):
        try: self.email = "%s@%s" % (environ['USER'], getfqdn())
        except: self.email = None
    
    def set_stage(self, stage=None):
        self.stage = "transfer.%s" % self.observatory
        if self.mode: self.stage += "-%s" % self.mode
        if stage: self.stage += ".%s" % stage
        self.set_ready()

    def set_mjd_dir(self):
        self.mjd_log_dir = join(self.staging,self.log_dir,str(self.mjd)) if self.staging and self.log_dir and self.mjd else None
        if self.mjd_log_dir:
            if exists(self.mjd_log_dir):
                if self.verbose: print("LOGGER> USING: %r" % self.mjd_log_dir)
            else:
                try:
                    makedirs(self.mjd_log_dir, self.perm)
                    if self.verbose: print("LOGGER> CREATE: %r" % self.mjd_log_dir)
                except Exception as e:
                    print("LOGGER> %r" % e)
                    self.mjd_log_dir = None
        
    def set_dir(self, dir = None):
        if dir: self.dir = dir
        elif self.mjd_log_dir and exists(self.mjd_log_dir):
            ls = listdir(self.mjd_log_dir)
            #n = max([int(d) for d in ls if not d.endswith('.json')]) + 1 if len(ls) > 0 else 0
            n = max([int(d) for d in ls if '.' not in d]) + 1 if len(ls) > 0 else 0
            self.dir = join(self.mjd_log_dir, str(n))
            try:
                makedirs(self.dir, self.perm)
                if self.verbose: print("LOGGER> CREATE: %r" % self.dir)
            except Exception as e:
                print("LOGGER> %r" % e)
                self.dir = None
        else: self.dir = None

    def set_file(self):
        log = "transfer.%s-%s.log" % (self.observatory, self.mode) if self.observatory and self.mode else "transfer.%s.log" % self.observatory if self.observatory else None
        self.file = join(self.dir, log) if self.dir and log else None
    
    def set_ready(self):
        if self.logger is None: self.set_logger()
        elif self.logger.handlers:
            if self.filehandler in self.logger.handlers: self.logger.removeHandler(self.filehandler)
            if self.smtphandler in self.logger.handlers: self.logger.removeHandler(self.smtphandler)
        self.set_filehandler()
        self.set_mailhosthandler()
        self.ready = True if self.dir is not None and self.logger is not None else False
    
    def set_logger(self):
         self.logger = logging.getLogger(self.stage) if self.stage else None
         if self.logger:
            if self.debug: self.logger.setLevel(logging.DEBUG)
            else: self.logger.setLevel(logging.INFO)

    def set_filehandler(self):
        if self.logger and self.file:
            self.filehandler = RotatingFileHandler(self.file, maxBytes=10485760, backupCount=5)
            format = "%(asctime)s - {stage} - %(levelname)s - %(message)s".format(stage=self.stage)
            formatter = logging.Formatter(format, self.time_format)
            self.filehandler.setFormatter(formatter)
            self.logger.addHandler(self.filehandler)

    def set_mailhosthandler(self):
        if self.logger and self.mailhost and self.email and self.recipients and self.stage:
            self.smtphandler = SMTPHandler(self.mailhost, self.email, self.recipients, "Critical error reported by %s for MJD=%r." % (self.stage, self.mjd))
            format = "At %(asctime)s, {stage} failed with this message:\n\n%(message)s\n\nSincerely, sdssadmin on behalf of the SDSS data team!".format(stage=self.stage)
            formatter = logging.Formatter(format, self.time_format)
            self.smtphandler.setFormatter(formatter)
            self.smtphandler.setLevel(logging.CRITICAL)
            self.logger.addHandler(self.smtphandler)

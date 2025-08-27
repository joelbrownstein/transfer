from configparser import ConfigParser
from os import environ
from os.path import join, exists
from time import time

class Config:

    def __init__(self, observatory = None, log_dir=None, ini_dir=None, ini_mode=None, verbose=None):
        self.observatory = observatory if observatory else "apo"
        self.mode = ini_mode
        self.verbose = verbose
        self.set_staging()
        self.set_log_dir(log_dir=log_dir)
        self.set_ini_dir(ini_dir=ini_dir)
        self.set_ini_file(ini_mode=ini_mode)
        self.set_options()

    def current_mjd(self): return int(time()/86400.0 + 40587.0)

    def set_staging(self):
        try: self.staging = environ["%s_STAGING_DATA" % self.observatory.upper() if self.observatory else None]
        except: self.staging = None
    
    def set_log_dir(self, log_dir=None):
        try: self.log_dir = log_dir if log_dir else 'atlogs'
        except: self.log_dir = None
        
    def set_ini_dir(self, ini_dir=None):
        try: self.ini_dir = ini_dir if ini_dir else environ['TRANSFER_INI_DIR']
        except: self.ini_dir = None
        
    def set_ini_file(self, ini_mode=None):
        self.ini_file = "transfer.%s_%s" % (self.observatory, ini_mode) if self.observatory and ini_mode else "transfer.%s" % self.observatory
        self.ini_file = join(self.ini_dir, self.ini_file) + ".ini"
        if not exists(self.ini_file): print("TRANSFER_CONFIG> Nonexistent path at %r" % self.ini_file)

    def set_options(self):
        self.options = None
        if self.ini_file and exists(self.ini_file):
            if self.verbose: print("TRANSFER_CONFIG> OPEN %s" % self.ini_file)
            try: 
                self.options = ConfigParser()
                r = self.options.read(self.ini_file)
                if len(r) !=1: self.options = None
            except Exception as e: print("TRANSFER_CONFIG> %r" % e)
        else: print("TRANSFER_CONFIG> cannot fine %r" % self.ini_file)
        if self.options:
            if self.verbose: print("TRANSFER_CONFIG> options.sections=%r" % self.options.sections())
        else: print("TRANSFER_CONFIG> No options")


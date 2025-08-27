from os import environ, getenv
from os.path import join, exists, basename
from pathlib import Path
from github import Github, Organization, GithubException
from netrc import netrc
from collections import OrderedDict
from datetime import datetime, timedelta
import json
import yaml
import glob
import requests
from re import findall
from itertools import chain
from yamlordereddictloader import Loader
from pytz import timezone


class GitHub:

    name = {'host': "github.com", 'organization': "sdss"}
    
    def __init__(self, options = None, key = None, branch = None, days = None, product = None, verbose = None):
        self.verbose = options.verbose if options else verbose
        self.set_key(key = options.key if options else key)
        self.name['branch'] = options.branch if options else branch
        self.name['product'] = self.product = options.product if options else product
        if not self.name['branch']: self.name['branch'] = "main"
        self.name['repo'] = join("%(organization)s", "%(product)s") % self.name if self.product else None
        self.days = options.days if options else days
        self.navajo = timezone('Navajo')
        self.date = None
        self.organization = self.members = None
        self.repo = self.branch = self.latest_commit = None
        self.workflows = self.workflow = self.run = None
        self.set_dir()
        self.set_vardir()
        self.set_github()
         
    def set_dir(self):
        self.dir = getenv('%s_DIR' % self.product.upper()) if self.product else None
        if self.dir and not exists(self.dir):
            if self.verbose: print("GITHUB> nonexistent directory %r" % self.dir)
            self.dir = None
        
    def set_vardir(self):
        try: self.vardir = environ['TRANSFER_VAR_DIR']
        except: self.vardir = join(self.dir, "var") if self.dir else None
        if self.vardir and not exists(self.vardir):
            if self.verbose: print("GITHUB> nonexistent var directory %r" % self.vardir)
            self.vardir = None

    def set_history(self):
        history = []
        if self.commits:
            for commit in self.commits:
                row = {'username': None}
                row['date'] = self.navajo.localize(commit.commit.committer.date).strftime('%Y-%m-%d %H:%M:%S')
                row['message'] = commit.commit.message
                row['person'] = commit.commit.committer.name
                row['email'] = commit.commit.committer.email
                row['sha'] = commit.commit.sha
                if row['person']:
                    if "blanton" in row['person'].lower(): row['username'] = "blanton144"
                    elif "joel" in row['person'].lower(): row['username'] = "joelbrownstein"
                    elif "morrison" in row['person'].lower(): row['username'] = "Sean-Morrison"
                    elif "cherinka" in row['person'].lower(): row['username'] = "havok2063"
                    elif "abigail" in row['person'].lower(): row['username'] = "OptXFinite"
                history.append(row)
        self.history = {self.date['date']: history} if self.date else history
        if self.verbose: print("GITHUB> history %r" % self.history)

    def set_latest_commit(self):
        self.latest_commit =  {'outdated': False} if self.days else {'outdated': True, 'sha': None}
        self.set_file(name = "commit-latest.json")
        if self.branch and self.file and self.latest_commit['outdated']:
            commit = self.branch.commit
            if exists(self.file):
                if self.verbose: print("GITHUB> OPEN %r" % self.file)
                with open(self.file, 'r') as file: latest_commit = json.load(file)
                if latest_commit: self.latest_commit = latest_commit
                self.latest_commit['outdated'] = ( self.latest_commit['sha'] != commit.sha )
            if self.latest_commit['outdated']:
                self.latest_commit['sha'] = commit.sha
                self.latest_commit['date'] = self.navajo.localize(commit.commit.committer.date).strftime('%Y-%m-%d %H:%M:%S')
                self.latest_commit['message'] = commit.commit.message
                self.latest_commit['person'] = commit.commit.committer.name
                self.latest_commit['email'] = commit.commit.committer.email
                if self.verbose: print("GITHUB> CREATE %r" % self.file)
                with open(self.file, 'w') as file: json.dump(self.latest_commit, file)
        
    def dump_history(self):
        name = "commit_history-%(date)s.json" % self.date if self.date else "commit-history.json"
        self.set_file(name = name)
        if self.history and self.file:
            if self.verbose: print("GITHUB> CREATE %r" % self.file)
            with open(self.file, 'w') as file: json.dump(self.history, file)
        

    def set_file(self, name = None):
        folder = "%(branch)s" % self.name if self.name else None
        try: self.file = join(self.vardir, folder, name) if self.vardir and folder and name else None
        except: self.file = None

    def set_netrc(self):
        try: self.netrc = netrc(file = getenv('NETRCFILE'))
        except Exception as e:
            if self.verbose: print("GITHUB> netrc %r" % e)
            self.netrc = None

    def set_key(self, key = None):
        if key: self.key = key
        else:
            self.set_netrc()
            if self.netrc:
                authenticators = self.netrc.authenticators(self.name['host'])
                if authenticators and len(authenticators) == 3:
                    self.key = authenticators[2] if authenticators[0] == "token" else None
                else:
                    if self.verbose: print("REMOTE> cannot find %(host)r in .netrc" % self.name)
                    self.key = None
            else: self.key = None
        
    def set_github(self):
        self.github = Github(self.key) if self.key else None
        
    def set_organization(self):
        org = self.name['organization'] if self.name and 'organization' in self.name else None
        self.organization = self.github.get_organization(org) if self.github and org else None
        
    def set_members(self):
        self.members = self.github.get_members() if self.github else None
        
    def set_repo(self):
        repo = self.name['repo'] if self.name and 'repo' in self.name else None
        self.repo = self.github.get_repo(repo) if self.github and repo else None
        if self.verbose: print("GITHUB> repo=%r" % self.repo)

    def set_branch(self):
        branch = self.name['branch'] if self.name and 'branch' in self.name else None
        try: self.branch = self.repo.get_branch(branch) if self.repo and branch else None
        except GithubException as e:
            self.branch = None
            print("GITHUB> Exception %(message)r" % e.data)
        if self.verbose: print("GITHUB> branch=%r" % self.branch)

    def set_workflows(self):
        self.workflows = self.repo.get_workflows() if self.repo else None
        if self.verbose: print("GITHUB> workflows=%r" % self.workflows)

    def set_workflow(self, name=None, run_index=None):
        self.set_workflows()
        workflow = [workflow for workflow in self.workflows if workflow.name == name] if self.workflows else None
        workflow = workflow[0] if workflow and len(workflow) == 1 else None
        try: runs = workflow.get_runs() if workflow else None
        except: runs = None
        run = [run for run in runs if run.head_branch == self.name['branch']] if runs else None
        try: run_index = int(run_index)
        except: run_index = 0
        run = run[run_index] if run and run_index < len(run) else None
        jobs_url = run.jobs_url if run else None
        try: jobs = json.loads(requests.get(jobs_url).text) if jobs_url else None
        except: jobs = None
        job = jobs['jobs'][0] if jobs and jobs['total_count'] == 1 else None
        success = job['conclusion'] != "failure" if "conclusion" in job else None
        self.workflow = {'run':run, 'job': job, 'workflow': workflow, 'success': success}
        if self.verbose: print("GITHUB> workflow=%(workflow)r --> success=%(success)r" % self.workflow)
            
    def set_date(self):
        now = datetime.now()
        midnight = datetime.combine(now, datetime.min.time())
        until = ( midnight - timedelta(days=self.days) ) if self.days else None
        date = (until if until else now).strftime("%Y%m%d")
        since = midnight - timedelta(days = 1 + self.days if self.days else 1)
        self.date = {'now':now, 'since': since, 'until': until, 'date': date}
        if self.verbose: print("GITHUB> date=%r" % self.date)
    
    def set_commits(self):
        branch = self.name['branch'] if self.name and 'branch' in self.name else None
        since = self.date['since'] if self.date else None
        until = self.date['until'] if self.date else None
        if since and self.repo and branch:
            self.commits = self.repo.get_commits(sha=branch, since=since, until=until) if until else  self.repo.get_commits(sha=branch, since=since)
            if self.verbose: print("GITHUB> commits=%r" % self.commits)
        else:
            self.commits = None

    def set_commit(self, sha = None):
        self.commit = self.repo.get_commit(sha=sha) if self.repo and sha else None

    def dump_commits(self):
        if self.days or self.latest_commit['outdated']:
            self.set_commits()
            self.set_history()
            self.dump_history()

    def touch_pull(self):
        if not self.days and self.latest_commit:
            if not self.workflow or self.workflow["success"]:
                self.set_file(name = "pull")
                if self.file:
                    path = Path(self.file)
                    if self.latest_commit['outdated']:
                        if self.verbose: print("GITHUB> touch %r" % self.file)
                        path.touch()
                    elif path.exists():
                        if self.verbose: print("GITHUB> remove %r" % self.file)
                        path.unlink()
            else:
                if self.verbose: print("GITHUB> nothing to do due to workflow failure" )

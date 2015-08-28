# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Provides pass/fail/aux-results reporting for specific test actions without
# necessarily affecting the entire test outcome.
#

import traceback
import inspect
import time

class TestAction (object):
    # Accumulated test actions in various forms.
    actions_root = list()
    actions = list()
    actions_by_fullname = dict()

    def __init__ (self, name, instance=None, desc=None, parent=None, logger=None):
        """
            name     := Appended to parent's name to create unique action identifier
            instance := Instance to test: allows multiple actions with the same name
            desc     := Human readable description of this action
            parent   := Parent action, either a TestAction object or a fully qualified
                        action name (as returned by TestAction.fullname).
            logger   := Logger to use. Only set this on your top-level action, it will be
                        inherited to all sub-actions.
        """
        
        super(TestAction, self).__init__()
        self.name       = name
        self.instance   = instance
        self.desc       = desc
        self.logger     = logger
        self.start_time = time.time()
        self.duration   = 999.999
        # Resolve parent
        if type(parent) == str:
            if parent in actions:
                parent = actions[parent]
            else:
                parent = None
        self.parent   = parent

        prefix=''
        if self.parent:
            # Add us to parent
            self.parent.subs.append(self)
            # Use parent's logger
            if not self.logger and self.parent.logger:
                self.logger = self.parent.logger
            # Prefix our name with parent's name
            prefix = self.parent.fullname + '.'


        # If there is no description but the calling function has a doc string, use that,
        # unless the parent action has the same one (a bit repetative).
        # FIXME: Doesnt seem to work for class functions
        #if not self.desc:
        #    cfr   = inspect.currentframe().f_back
        #    cname = cfr.f_code.co_name
        #    doc   = cfr.f_globals[cname].__doc__
        #    if not self.parent or doc != self.parent.desc:
        #        self.desc = doc

        if instance != None:
            self.fullname = '%s%s@%s' % (prefix, name, instance)
        else:
            self.fullname = prefix + name

        if not self.logger:
            self.logger = self._dummy_logger

        self.subs     = list()   # Sub actions
        self.result   = list()
        self.fail_cnt = 0
        self.pass_cnt = 0
        self.warn_cnt = 0

        if self.fullname in self.actions_by_fullname:
            raise Exception('TestAction(%s) already exists' % self.fullname)

        self.actions.append(self)
        self.actions_by_fullname[self.fullname] = self

        if not self.parent:
            self.actions_root.append(self)

        self.logger.info('TestAction "%s" BEGIN' % self.fullname)
        

    def _dummy_logger (self, *args):
        pass

    def __enter__ (self):
        return self

    def __exit__ (self, exc_type, exc_value, trace):
        if exc_type == None:
            self.done()
        else:
            self.failed('Exception %s: %s: at %s' % (exc_type, exc_value, ''.join(traceback.format_tb(trace))))
        return True

    def add_result (self, restype, reason):
        """ Add result information of any type to this action. May be called multiple times. """
        self.duration = time.time() - self.start_time
        info = inspect.getframeinfo(inspect.stack()[2][0])
        loc = '%s:%d:%s()' % (info.filename, info.lineno, info.function)
        self.result.append({'restype': restype, 'reason': reason, 'loc': loc})
        self.logger.info('TestAction "%s" RESULT: %s' % (self.fullname, restype.upper()))

    def passed (self, reason):
        """ Pass this action with the provided reason """
        self.add_result('pass', reason)
        self.pass_cnt += 1
        self.logger.info('Action "%s" PASSED: %s' % (self.fullname, reason))

    def failed (self, reason):
        """ Fail this action with the provided reason """
        self.add_result('fail', reason)
        self.fail_cnt += 1
        self.logger.error('Action "%s" FAILED: %s' % (self.fullname, reason))

    def warning (self, reason):
        """ Logs a warning for the current action, adds the same as a 'WARN' result.
            Use this for non-critical errors that may still affect the outcome of the test action,
            e.g., environment anomalies. """
        self.add_result('warn', reason)
        self.warn_cnt += 1
        self.logger.warning('Action "%s" WARNING: %s' % (self.fullname, warning))

    def done (self):
        """ Finishes the action with either passed or failed depending on if any
            sub actions failed or not. """
        if self.is_done():
            return # Already done
        failed = sum([x.fail_cnt for x in self.subs])
        if failed > 0:
            self.fail_cnt += 1
        else:
            self.pass_cnt += 1
        self.duration = time.time() - self.start_time


    def has_failed (self):
        return self.fail_cnt > 0

    def has_passed (self):
        return self.pass_cnt > 0 and self.fail_cnt == 0

    def is_done (self):
        return self.has_failed() or self.has_passed()

    def status (self):
        """ Returns the status string for this action """
        if not self.has_failed() and not self.has_passed():
            return 'dnf'
        elif self.has_failed():
            return 'fail'
        elif self.has_passed():
            return 'pass'
        else:
            return 'partial'

    def get_result (self):
        """ Populate a result dict for this action and all its sub-actions.
            Returns a list of such dicts. """
        res = dict()
        res['name'] = self.name
        res['fullname'] = self.fullname
        res['instance'] = self.instance
        res['desc'] = self.desc
        res['status'] = self.status()
        res['results'] = self.result
        res['warncnt'] = self.warn_cnt
        res['start_time'] = self.start_time
        res['duration'] = '%.3f' % float(self.duration)
        res['subcnt'] = len(self.subs)

        res_list = [res]
        if len(self.subs) > 0:
            for sub in self.subs:
                res_list.extend(sub.get_result())
        return res_list

    def report (self, reporter):
        """ *ActionReporter() feeder
            FIXME: This should be consolidated with get_results() above """
        reporter.add_row()
        reporter.add('name', self.name)
        reporter.add('fullname', self.fullname)
        reporter.add('instance', self.instance)
        reporter.add('desc', self.desc)
        reporter.add('status', self.status())
        reporter.add('results', self.result)
        reporter.add('warncnt', self.warn_cnt)
        reporter.add('start_time', self.start_time)
        reporter.add('duration', self.duration)
        reporter.add('subcnt', len(self.subs))
        if len(self.subs) > 0:
            reporter.push_stack()
            for sub in self.subs:
                sub.report(reporter)
            reporter.pop_stack()



    @staticmethod
    def results ():
        """ Return a list containing result dicts for all accumulated TestActions so far """
        res = list()

        for a in TestAction.actions_root:
            res.extend(a.get_result())

        return res



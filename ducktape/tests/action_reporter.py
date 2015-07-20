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
# Reporters for TestAction
#

from action import TestAction

class TestActionReporter (object):
    def __init__ (self):
        super(TestActionReporter, self).__init__()
        self.stack_depth = 0
        self.areps = list()  # per-Action reports
        self.curr = None     # current arep (action report)
        self.longest_name = 0
        self.longest_fullname = 0

    def push_stack (self):
        """ Prepare for sub-action """
        self.stack_depth += 1

    def pop_stack (self):
        """ Return for sub-action """
        if self.stack_depth == 0:
            raise Exception('Too many pop_stack(), already at depth 0')
        self.stack_depth -= 1

    def add_row (self):
        """ Add new test result row """
        self.curr = dict()
        self.curr['depth'] = self.stack_depth
        self.areps.append(self.curr)

    def add (self, key, value):
        """ Add generic key=value to row. """
        if key in self.curr:
            raise Exception('Multiple add()s for key %s (prev value %s)' % (key, str(self.curr[key])))
        self.curr[key] = value
        if key == 'name' and len(value) > self.longest_name:
            self.longest_name = len(value)
        if key == 'fullname' and len(value) > self.longest_fullname:
            self.longest_fullname = len(value)

    def render_row (self, row):
        """ Render a single row, return string """
        raise Exception('Must be implemented by sub class')

    def render (self, **kwargs):
        """ Render all rows, returns list of rendered rows """
        res = list()
        for a in self.areps:
            res.append(self.render_row(a))
        return res

    def report (self):
        """ Create test action report, returns a list of rendered rows """
        # Add all TestActions to report
        for a in TestAction.actions_root:
            a.report(self)

        # Render report
        res = self.render()
        return res


class TextActionReporter (TestActionReporter):
    """ Text/console reporter """
    def __init__ (self):
        super(TextActionReporter, self).__init__()

    def render_row (self, row):
        """ Render single row """
        #indent = row['depth']
        colors = {'PASS': '\033[32m', 'FAIL': '\033[31m'}
        if row['status'] in colors:
            color = colors[row['status']]
            color_rst = '\033[0m'
        else:
            color = ''
            color_rst = ''
        indent = 0
        ret  = ' ' * indent
        fmt  = '%%-%ds: ' % self.longest_fullname
        ret += color
        ret += fmt % row['fullname']
        ret += '%-4s : ' % row['status']
        ret += '%8s : ' % ('%.3f' % row['duration'])
        ret += '%s' % ', '.join([x['reason'] for x in row['results']])
        ret += color_rst
        return ret

    def header (self):
        """ Return column header """
        fmt  = '%%-%ds: ' % self.longest_fullname
        ret  = fmt % 'Action'
        ret += '%-4s : ' % 'STAT'
        ret += '%8s : ' % 'DURATION'
        ret += '%s' % 'Results'
        return ret
    
    def report (self):
        """ Returns a full report as string """
        res = super(TextActionReporter, self).report()

        ret = self.header() + '\n'
        ret += '\n'.join(res)
        return ret


class HtmlActionReporter (TestActionReporter):
    """ HTML report """
    def __init__ (self):
        super(HtmlActionReporter, self).__init__()

    def render_row (self, row):
        """ Render single row """
        ret  = '<tr><td>%s</td>' % row['fullname']
        ret += '<td>%s</td>' % row['status']
        ret += '<td>%.3f</td>' % row['duration']
        ret += '<td><pre>%s</pre></td>' % '<br>\n'.join([x['reason'] for x in row['results']])
        return ret

    def report (self):
        """ Returns a full report as HTML string """
        ret  = '<html><body>'
        ret += '<table>'
        res = super(HtmlActionReporter, self).report()
        ret += '\n'.join(res)
        ret += '</table>'
        ret += '</body></html>'
        return ret

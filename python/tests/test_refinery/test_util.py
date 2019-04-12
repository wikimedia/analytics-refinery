from unittest import TestCase
from refinery.util import sh


class TestRefineryUtil(TestCase):
    def test_sh(self):
        command = ['/bin/echo', 'test-list']
        output = sh(command)
        self.assertEqual(output, 'test-list')

        command = '/bin/echo test-string'
        output = sh(command)
        self.assertEqual(output, 'test-string')

    def test_sh_pipe(self):
        command = '/bin/echo hi_there | /usr/bin/env sed -e \'s@_there@_you@\''
        output = sh(command)
        self.assertEqual(output, 'hi_you')

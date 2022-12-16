import unittest
import bard_runner
#from unittest.mock import patch
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

import sys



class TestRunCommand(unittest.TestCase):
    def test_main(self):
        #args = ['nextflow', '-log', 'None', '-syslog', 'None', 'run', '/mnt/c/Users/abc/Desktop/Python_scripts/opt/pipeline/main_bard.nf', '-name', 'BARD_2b5338a09cf54e7e86c8653942bc223a', '-c', '/opt/nf-runner/nf-runner.config', '--tier', 'ruo', '--assay_type', 'wgs', '--variant_type', 'genome', '-profile', 'standard','cc']
        #args = ['nextflow', '--tier', 'ruo', '--assay_type', 'wgs', '--variant_type', 'genome', '-profile', 'standard']
        testargs = ['nextflow', '--tier', 'ruo', '--assay_type', 'wgs', '--variant_type', 'genome']

        with patch.object(sys, 'argv', testargs):
            self.nf_command = bard_runner.main()
            #nf_command = 'nextflow -log None -syslog None run /mnt/c/Users/abc/Desktop/Python_scripts/opt/pipeline/main_bard.nf -name BARD_2b5338a09cf54e7e86c8653942bc223a -c /opt/nf-runner/nf-runner.config --tier ruo --assay_type wgs --variant_type genome -profile standard,cc'

            assert self.nf_command == '/mnt/c/Users/abc/Desktop/Python_scripts/bard_runner.py --tier ruo --assay_type wgs --variant_type genome'
            #assert self.nf_command == 'nextflow -log None -syslog None run /mnt/c/Users/abc/Desktop/Python_scripts/opt/pipeline/main_bard.nf -name BARD_2b5338a09cf54e7e86c8653942bc223a -c /opt/nf-runner/nf-runner.config --tier ruo --assay_type wgs --variant_type genome -profile standard,cc'

if __name__ == '__main__':
    unittest.main()
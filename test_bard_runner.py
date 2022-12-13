import unittest
from bard_runner import PipelineCaller
import bard_runner

class TestRunCommand(unittest.TestCase):
    def test_main(self):
        self.main_command = bard_runner.main()
        nf_command = 'nextflow -log None -syslog None run /mnt/c/Users/abc/Desktop/Python_scripts/opt/pipeline/main_bard.nf -name BARD_2b5338a09cf54e7e86c8653942bc223a -c /opt/nf-runner/nf-runner.config --tier ruo --assay_type wgs --variant_type genome -profile standard,cc'
        self.assertEqual(self.main_command, nf_command)

if __name__ == '__main__':
    unittest.main()
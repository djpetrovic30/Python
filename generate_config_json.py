import json
import argparse
import os
import sys
import subprocess
from pathlib import Path

# since booleans will come from the json as lowercase
true = True
false = False

# run the `nextflow config` command and return stdout
def generate_config(pipeline_dir: Path, tier: str):
	cmd = f'nextflow config -flat'
	result = subprocess.run(cmd, env={'NXF_TIER': tier, **os.environ}, cwd=pipeline_dir, shell=True, check=True, stdout=subprocess.PIPE)
	config_str = result.stdout.decode('utf-8')
	return config_str

# so we can set params.definitions.param.rule in one shot
class NestedDict(dict):
	def __missing__(self, key):
		self[key] = NestedDict()
		return self[key]

def parse_config(config_str):
	lines = config_str.splitlines()
	params = NestedDict()
	for line in lines:
		if line.startswith('params'): # this was in its own function but it run into scope issues with params
			x, val = line.split(' = ')
			levels = x.split('.')
			ks = [ f"['{k}']" for k in levels[1:] ] # levels[0] is 'params'
			cmd = f"params{''.join(ks)} = {val}"
			exec(cmd)
	return params

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', '--dir', type=str, default='/opt/pipeline')
	parser.add_argument('-t', '--tiers', type=str, nargs='+', required=True)
	args = parser.parse_args()
	pipeline_dir = Path(args.dir)
	if not pipeline_dir.is_dir():
		raise Exception(f'--dir {pipeline_dir} is not a valid directory')
	nf_config = Path(f'{pipeline_dir}/nextflow.config')
	if not (nf_config.exists() and nf_config.is_file()):
		raise Exception(f'could not find nextflow.config file in --dir {pipeline_dir}')
	Path(f'{pipeline_dir}/param_jsons').mkdir(exist_ok=True)
	for tier in args.tiers:
		print(f'working on {tier} tier')
		cfg = generate_config(pipeline_dir, tier)
		params = parse_config(cfg)
		fname = f'{pipeline_dir}/param_jsons/{tier}_params.json'
		print(f'creating {fname}')
		with open(f'{fname}', 'w') as f:
			json.dump(params, f, indent=2)
	# not sure why Path().mkdir() permissions are so stubborn
	subprocess.call(['chmod', '-R', '755', f'{pipeline_dir}/param_jsons'])

if __name__ == '__main__':
	sys.exit(main())

#!/usr/bin/env python
import sys
import pandas as pd
import numpy as np
from argparse import ArgumentParser
import json
import collections

def parse_args(args_list):
	parser = ArgumentParser()
	parser.add_argument('-a', '--align', type=str, required=True, help='bam alignment metrics file')
	parser.add_argument('-i', '--insert', type=str, required=True, help='bam insert size metrics file')
	parser.add_argument('-d', '--dedup', type=str, required=False, help='bam duplication metrics file')
	parser.add_argument('-p', '--picard_metrics', type=str, required=True, help='assay metrics file from picard')
	parser.add_argument('-m', '--mnp_metrics', type=str, required=True, help='assay metrics file from TNscope or DNscope')
	parser.add_argument('-b', '--tmb_metrics', required=False, default=None, help='assay metrics file from TMB')
	parser.add_argument('-n', '--purecn_metrics', required=False, default=None, help='assay metrics file from PureCN')
	parser.add_argument('-t', '--assay_type', type=str, required=True, help='assay type', choices=['WGS', 'wgs', 'WES', 'wes', 'panel', 'amplicon'])
	parser.add_argument('-o', '--output', type=str, required=True, help='base name for output .txt and .json files')
	return parser.parse_args(args_list)

# alignment stats
def parse_align(filename):
	if filename:
		df = pd.read_csv(filename, sep='\t', comment='#', header=0, index_col=0)
		df_out = pd.DataFrame({
			'Category': 'alignment',
			'Metric': 'pct_aligned',
			'Value': df.loc['PAIR', 'PCT_READS_ALIGNED_IN_PAIRS'] * 100
		}, index=[0])
		return df_out
	else:
		return None

# insert size stats
def parse_insert(filename):
	if filename:
		series = pd.read_csv(filename, sep='\t', comment='#', header=0, index_col=False, nrows=1).squeeze()
		df_out = pd.DataFrame({
			'Category': ['alignment'] * 2,
			'Metric': ['mean_insert_size', 'median_insert_size'],
			'Value': [series.MEAN_INSERT_SIZE, series.MEDIAN_INSERT_SIZE]
		})
		return df_out
	else:
		return None

# duplication stats
def parse_dedup(filename):
	if filename:
		series = pd.read_csv(filename, sep='\t', comment='#', header=0, index_col=0, nrows=1).squeeze()
		df_out = pd.DataFrame({
			'Category': ['duplication'] * 2,
			'Metric': ['pct_duplication', 'estimated_library_size'],
			'Value': [series.PERCENT_DUPLICATION * 100, series.ESTIMATED_LIBRARY_SIZE]
		})
		return df_out
	else:
		return None

# picard assay performance stats
def parse_picard_metrics(filename, assay_type):
	if filename:
		series = pd.read_csv(filename, sep='\t', comment='#', nrows=1).squeeze()
		x = series.index.values[0] # first column in file can tell us which tool was used
		if x == 'GENOME_TERRITORY' and assay_type.lower() == 'wgs':
			return parse_wgs_metrics(series)
		elif x == 'BAIT_SET' and assay_type.lower() == 'wes':
			return parse_wes_metrics(series)
		elif x == 'BAIT_SET' and assay_type == 'panel':
			return parse_panel_metrics(series)
		elif x == 'CUSTOM_AMPLICON_SET' and assay_type == 'amplicon':
			return parse_pcr_metrics(series)
		else:
			raise Exception(f'could not parse picard assay performance metrics from {filename}. It may be formatted incorrectly, or it may not match assay type {assay_type}')
	else:
		return None

def parse_wgs_metrics(series):
	df_out = pd.DataFrame({
		'Category': ['assay_performance'] * 7,
		'Metric': ['assay_type', 'mean_coverage', 'median_coverage', 'pct_1x', 'pct_10x', 'pct_20x', 'pct_30x'],
		'Value': ['WGS', series.MEAN_COVERAGE, series.MEDIAN_COVERAGE, 100*series.PCT_1X, 100*series.PCT_10X, 100*series.PCT_20X, 100*series.PCT_30X]
	})
	return df_out

def parse_wes_metrics(series):
	df_out = pd.DataFrame({
		'Category': ['assay_performance'] * 7,
		'Metric': ['assay_type', 'mean_target_coverage', 'median_target_coverage', 'pct_target_1x', 'pct_target_20x', 'pct_target_50x', 'pct_target_100x'],
		'Value': ['hybrid selection', series.MEAN_TARGET_COVERAGE, series.MEDIAN_TARGET_COVERAGE, 100*series.PCT_TARGET_BASES_1X, 100*series.PCT_TARGET_BASES_20X, 100*series.PCT_TARGET_BASES_50X, 100*series.PCT_TARGET_BASES_100X]
	})
	return df_out

def parse_panel_metrics(series):
	df_out = pd.DataFrame({
		'Category': ['assay_performance'] * 8,
		'Metric': ['assay_type', 'mean_target_coverage', 'median_target_coverage', 'pct_target_1x', 'pct_target_100x', 'pct_target_250x', 'pct_target_500x', 'pct_target_1000x'],
		'Value': ['hybrid selection', series.MEAN_TARGET_COVERAGE, series.MEDIAN_TARGET_COVERAGE, 100*series.PCT_TARGET_BASES_1X, 100*series.PCT_TARGET_BASES_100X, 100*series.PCT_TARGET_BASES_250X, 100*series.PCT_TARGET_BASES_500X, 100*series.PCT_TARGET_BASES_1000X]
	})
	return df_out

def parse_pcr_metrics(series):
	df_out = pd.DataFrame({
		'Category': ['assay_performance'] * 8,
		'Metric': ['assay_type', 'mean_target_coverage', 'median_target_coverage', 'pct_target_1x', 'pct_target_100x', 'pct_target_500x', 'pct_target_1000x', 'pct_target_5000x'],
		'Value': ['targeted PCR', series.MEAN_TARGET_COVERAGE, series.MEDIAN_TARGET_COVERAGE, 100*series.PCT_TARGET_BASES_1X, 100*series.PCT_TARGET_BASES_100X, 100*series.PCT_TARGET_BASES_500X, 100*series.PCT_TARGET_BASES_1000X, 100*series.PCT_TARGET_BASES_5000X]
	})
	return df_out

# parse dnascope metrics
# OR parse tnscope metrics
def parse_mnp_metrics(filename):
	series = pd.read_csv(filename, sep='\t', comment='#').squeeze()
	df_out = pd.DataFrame({
			# 'Category': ['variant_calling'] * 2,
		# 'Metric': ['total_snps', 'novel_snps'],
		# 'Value': [series.TOTAL_SNPS, series.NOVEL_SNPS]
		'Category': ['variant_calling'] * 2,
		'Metric': ['total_snps','total_indels'],
		'Value': [series.TOTAL_SNPS, series.TOTAL_INDELS]
	})
	return df_out

def parse_tmb_metrics (filename):
	if filename == None:
		return None
	series = pd.read_csv(filename, sep='\t').squeeze()
	columns = [col.replace(" ", "_") for col in  
	['Number of mutations', 'total size of region filtered by depth', 'total size of region', 'TMB']]
	if "TMB" in series.index:
		df_out = pd.DataFrame({
			'Category': ['tumor_mutational_burden'] * len(series),
			'Metric': columns,
			'Value': [
				series['Number of mutations'],
				series['total size of region filtered by depth'],
				series['total size of region'],
				series['TMB']
				]
		})

	else: # if errs
		df_out = pd.DataFrame({
			'Category': ['tumor_mutational_burden'],
			'Metric': ['TMB'],
			'Value': [np.nan]
		})
	
	return df_out 


def parse_purecn_metrics (filename):
	if filename == None:
		return None
	series = pd.read_csv(filename, sep=',').squeeze()
	df_out = pd.DataFrame({
			'Category': ['tumor-only_diagnostic'] * 4,
			'Metric': ['Purity','Ploidy','Flagged','Comment'],
			'Value': [series.Purity, series.Ploidy, series.Flagged, series.Comment]
	})
	return df_out 

# utils
# https://stackoverflow.com/questions/50916422/python-typeerror-object-of-type-int64-is-not-json-serializable
class NpEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, np.integer):
			return int(obj)
		if isinstance(obj, np.floating):
			return float(obj)
		if isinstance(obj, np.ndarray):
			return obj.tolist()
		return super(NpEncoder, self).default(obj)

def conditional_round(x):
	try:
		y = float(x)
		if y%1 == 0:
			return int(x)
		else:
			return round(y, 6) # picard tools go to 6 digits, anything longer is just floating point precision stuff
	except ValueError:
			return x


def join_dfs(*args):
	dfs = [x for x in args if x is not None]
	if len(dfs) == 0:
		raise Exception('could not find any data frames to join')
	joined_dfs = pd.concat(dfs)
	df = joined_dfs.set_index(['Category', 'Metric'])
	df['Value'] = df['Value'].map(conditional_round)
	return df

def save_txt(df, output_path):
	merged_df = df.copy()
	merged_df.index = merged_df.index.map("--".join)
	merged_df.index.name = "Metric"
	merged_df.to_csv(output_path + ".txt", sep='\t', float_format='%.1f')

def save_json(df, output_path):
	out = output_path + ".json"
	nested_dict = collections.defaultdict(dict)
	for keys, value in df.Value.iteritems():
		nested_dict[keys[0]][keys[1]] = value
	with open(out, 'w') as outfile:
		json.dump(nested_dict, outfile, cls=NpEncoder, indent=4)

def main():
	args 		= parse_args(sys.argv[1:])
	align_df 	= parse_align(args.align)
	insert_df 	= parse_insert(args.insert)
	dedup_df 	= parse_dedup(args.dedup)
	picard_df 	= parse_picard_metrics(args.picard_metrics, args.assay_type)
	mnp_df 		= parse_mnp_metrics(args.mnp_metrics)
	tmb_df 		= parse_tmb_metrics(args.tmb_metrics)
	purecn_df 	= parse_purecn_metrics(args.purecn_metrics)
	final_df 	= join_dfs(align_df, insert_df, dedup_df, picard_df, mnp_df, tmb_df, purecn_df)
	save_txt(final_df, args.output)
	save_json(final_df, args.output)

if __name__ == '__main__':
	sys.exit(main())
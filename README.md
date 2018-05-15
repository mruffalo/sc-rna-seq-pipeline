# scRNA-seq alignment pipeline

`process_sra_from_srr_id.py` provides a command-line interface for downloading,
and aligning scRNA-seq data from the NCBI GEO system, and quantifying gene
expression. This script takes two command-line arguments:

1. Path to file containing SRR IDs, one ID per line (required)
1. `-s` or `--subprocesses`: passed directly to the HISAT2 aligner

`cluster_scheduling.py` is provided for convenience; this script automatically
schedules batch jobs on a computational cluster running Slurm. It is designed
for the CMU Computational Biology Department's Lane cluster, and will likely
need adjustments for other systems.

# Supporting Website

http://scquery.cs.cmu.edu/

# Requirements

* Python 3.6 or newer
* Extra Python packages: `data-path-utils`, `intervaltree`, `pandas`
* HISAT2, version 2.1.0 or newer

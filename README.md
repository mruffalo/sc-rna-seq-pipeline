# scRNA-seq alignment pipeline

This project comprises a standardized alignment pipeline for single-cell RNA-seq
reads, mapping these reads to locations of known genes, and saving gene expression
and alignment metadata in HDF5 format. This data is suitable for input in the
scQuery system at https://scquery.cs.cmu.edu/ .

Multiple command-line scripts are provided, for different input types. These
scripts share many command-line options, and ultimately save gene expression and
alignment metadata in HDF5 format.

1. `process_fastq_file.py` takes one or two FASTQ files as input: one file for
   single-end, and two files for paired-end.
1. `process_fastq_directory.py` searches for FASTQ files in a given directory,
   identifies paired-end files via `_1.fastq` and `_2.fastq` suffixes, aligns
   all single- or paired-end files found, and saves a single output file with
   gene expression for all FASTQ files.
1. `process_sra_file.py` is similar to `process_fastq_file.py`, but converts
   reads from SRA to FASTQ format before alignment. Single- and paired-end data
   is automatically detected.
1. `process_sra_directory.py` processes all SRA files in a given directory.

These scripts share some command-line arguments:

* `-s` or `--subprocesses`: number of subprocesses to use for alignment.
* `--reference-path`: path on disk of the HISAT2 index. Note that this path is
  not of any actual file on disk, but is the "base name" of the HISAT2 index.
  For instance, if the index is labeled "mm10" and is stored in the
  `/path/to/index` directory as files `mm10.1.ht2` through `mm10.8.ht2`, the
  reference path would be `/path/to/index/mm10`.
* `--output-file`: where to save the gene expression and alignment metadata.
  If omitted, scripts will save alignment results to an appropriate place. See
  each script's `--help` information for details.
* `--hisat2-options`: extra options passed directly to HISAT2. This must be
  provided as a single string; this value will be split on whitespace and each
  piece will be a separate argument/option to HISAT2. For multiple options,
  it will probably be necessary to surround the option string in quotes. For
  example: `--hisat2-options="--mp 4,2 --phred64"`

# Data Requirements

After alignment to a reference genome, reads are mapped to genes via a mapping from
chromosome names to interval tree indexes. This index is built from the NCBI
Consensus CDS (CCDS) data, and the current CCDS database for mouse is available at
ftp://ftp.ncbi.nih.gov/pub/CCDS/current_mouse/CCDS.current.txt

If desired, one can build this index from a local copy of the CCDS database, using
the `build_tree.py` script. For convenience, a prebuilt index is also available at
https://giygas.compbio.cs.cmu.edu/mouse-ccds-index.tar.xz -- this archive should
be extracted to the same directory containing this README file.

# Software Requirements

* Python 3.6 or newer
* Extra Python packages: `data-path-utils`, `intervaltree`, `pandas`, `pytables`
* HISAT2, version 2.1.0 or newer
* NCBI SRA Toolkit, if working with SRA files

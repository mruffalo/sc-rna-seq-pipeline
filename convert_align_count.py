#!/usr/bin/env python3
import argparse
from subprocess import check_call


def main(geo_id):
    # Build up the interval tree of gene locations on chromosomes for once.
    tree_command = [
        'python3',
        'build_tree.py'
    ]
    # check_call(tree_command)

    # Convert SRA files into FASTQ files.
    fastq_pieces = [
        '{fastq_path}',
        '{input_path}',
        '-O',
        '{output_path}'
    ]
    fastq_command = [
        piece.format(fastq_path='/home/zhilinh/tools/sratoolkit.2.8.2-1-centos_linux64/bin/fastq-dump',
                     input_path='/home/zhilinh/data/' + geo_id + '.sra',
                     output_path='/home/zhilin/data/')
        for piece in fastq_pieces
    ]
    check_call(fastq_command)

    # Align FASTQ files to the mouse genome.
    hisat_pieces = [
        '{hisat2_path}',
        '-x',
        '{reference_path}',
        '-U',
        '{input_path}',
        '-S',
        '{output_path}'
    ]
    hisat_command = [
        piece.format(hisat2_path='/home/zhilinh/tools/hisat2-2.1.0/hisat2',
                     reference_path='/home/zhilinh/alignment/hisat2/combined2',
                     input_path='/home/zhilinh/data/' + geo_id + '.fastq',
                     output_path='/home/zhilinh/data/' + geo_id + '.sam')
        for piece in hisat_pieces
    ]
    check_call(hisat_command)

    # Count reads and print out stats.
    count_piece = [
        'python3',
        'parse_gene_intervals.py',
        '{name}'
    ]
    count_command = [
        piece.format(name=geo_id)
        for piece in count_piece
    ]
    check_call(count_command)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'geo_id', default='', help='GEO_ID')
    args = parser.parse_args()
    main(args.geo_id)

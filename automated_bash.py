#!/usr/bin/env python3
import subprocess
import os
from subprocess import check_call


def main():
    # subprocess.run("fastq-dump /home/zhilinh/data/" + geoid, shell=True)
    # subprocess.run("hisat2 -x /home/zhilinh/alignment/hisat2/combined2 -U /home/zhilinh/data/" + geoid
    #                + ".fastq -S /home/zhilinh/reads.sam -p 24", shell=True)
    # subprocess.run("python3 parse_gene_intervals.py", shell=True)
    # subprocess.run("python3 counts.py", shell=True)

    # Build up the interval tree of gene locations on chromosomes for once.
    tree_command = [
        'python3',
        'build_tree.py'
    ]
    check_call(tree_command)

    # Walk through all SRA files in the directory
    for root, dirs, files in os.walk("/scratch/lin/data/"):
        for name in files:
            if ".sra" in name:
                # Convert SRA files into FASTQ files.
                fastq_pieces = [
                    '{fastq_path}',
                    '{input_path}',
                    '-O',
                    '{output_path}'
                ]
                fastq_command = [
                    piece.format(fastq_path='/home/zhilinh/tools/sratoolkit.2.8.2-1-centos_linux64/bin/fastq-dump',
                                 input_path=os.path.join(root, name),
                                 output_path='/scratch/lin/data/')
                    for piece in fastq_pieces
                ]
                check_call(fastq_command)

                # Align FASTQ files to the mouse genome using 24 cores.
                hisat_pieces = [
                    '{hisat2_path}',
                    '-x',
                    '{reference_path}',
                    '-U',
                    '{input_path}',
                    '-S',
                    '{output_path}',
                    '-p',
                    '24'
                ]
                hisat_command = [
                    piece.format(hisat2_path='/home/zhilinh/tools/hisat2-2.1.0/hisat2',
                                 reference_path='/home/zhilinh/alignment/hisat2/combined2',
                                 input_path=os.path.join(root, name)[:-4] + '.fastq',
                                 output_path='/scratch/lin/' + name[:-4] + '.sam')
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
                    piece.format(name=name[:-4])
                    for piece in count_piece
                ]
                check_call(count_command)

if __name__ == '__main__':
    main()

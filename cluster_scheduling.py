from subprocess import check_call
from pathlib import Path

script_template = """
#!/bin/bash

#SBATCH -p zbj1
#SBATCH --mem=4096
#SBATCH --mincpus=1

wget {ftp_url} -P /scratch/zhilinh/data/
python3 /home/zhilinh/automated_bash.py
""".strip()

slurm_command = [
    'sbatch',
    '{script_filename}',
]

# Import the list of URLs.
ftp_list_file = Path('/home/zhilinh/sra-ftp-paths.txt').expanduser()

with open(ftp_list_file) as f:
    for line in f:
        # strip off ".sra" extension.
        ftp_url = line.strip()
        slash_index = ftp_url.rfind('/')
        geo_id = ftp_url[slash_index + 1:][:-4]
        script_path = '/home/zhilinh/data/' + geo_id + '.sh'
        sra_path = '/home/zhilinh/data/' + geo_id + '.sra'
        # Fill in the script with SRA file path and URL.
        slurm_script = script_template.format(sra_path=sra_path, ftp_url=ftp_url)

        # Create the bash script by the script_template.
        bash_script = open(script_path, 'w')
        bash_script.write(slurm_script)
        bash_script.close()

        # Not sure if permission needed.
        permission = [
            'chmod',
            '+x',
            '{script_filename}'
        ]
        permission_command = [piece.format(script_filename=script_path) for piece in permission]
        check_call(permission_command)

        # Run the bash script just crated for every SRA file.
        command = [piece.format(script_filename=script_path) for piece in slurm_command]
        print('Running', ' '.join(command))
        check_call(command)

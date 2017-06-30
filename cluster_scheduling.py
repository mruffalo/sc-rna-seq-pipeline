from subprocess import check_call
from pathlib import Path

script_template = """
#!/bin/bash

#SBATCH -p zbj1
#SBATCH --mem=4096
#SBATCH --mincpus=1

wget {ftp_url} -P /home/zhilinh/data
python /home/zhilinh/convert_align_count.py {geo_id}
""".strip()

slurm_command = [
    'sbatch',
    '{script_filename}',
]

# Import the list of URLs.
ftp_list_file = Path('/home/zhilinh/sra-ftp-paths.txt').expanduser()

with open(ftp_list_file) as f:
    for line in f:
        # strip off ".sra" extension, save to "SRR*.sh".
        line = line.strip()
        index = line.rfind('/')
        geo_id = line[index + 1:][:-4]
        script_filename = '/home/zhilinh/data/' + geo_id + '.sh'
        # Fill in the script with each URL.
        slurm_script = script_template.format(ftp_url=line, geo_id=geo_id)

        # Create the bash script by the script_template
        bash_script = open(script_filename, 'w')
        bash_script.write(slurm_script)
        bash_script.close()

        # Not sure if permission needed
        permission = [
            'chmod',
            '+x',
            '{script_filename}'
        ]
        permission_command = [piece.format(script_filename=script_filename) for piece in permission]
        check_call(permission_command)

        command = [piece.format(script_filename=script_filename) for piece in slurm_command]
        print('Running', ' '.join(command))
        check_call(command)

import json
from pathlib import Path
import re
from uuid import uuid4


# What to include and exclude?
include, exclude = [], []
with open('bac120_taxonomy_r207.tsv', 'r') as file:
    for line in file:

        ID, taxonomy = line.strip().split('\t')
        if ID[:3] == 'RS_':
            ID = ID.replace('RS_', '')
        elif ID[:3] == 'GB_':
            ID = ID.replace('GB_', '')
        else:
            raise ValueError('Hm.')

        d = {}

        for i in taxonomy.split(';'):
            rank, name = i.split('__')
            d[rank] = name

        if d['f'] == 'Legionellaceae':
            if d['s'] == 'Legionella pneumophila':
                include.append(ID)
            else:
                exclude.append(ID)


p = Path('genomes')
files = p.rglob('*.fna.gz')
rules_in, rules_ex = [], []
# i_delta, o_delta = [], []
with open('input.csv', 'w+') as out:
    for i in files:
        ID = uuid4().__str__().split('-')[0]
        fp = i.resolve().__str__()
        x = re.match('.*(GC[FA]_.*?\.\d)_.*', fp).group(1)
        if x in include:
            # The name we write to the rules has to match the annotation
            # labels in the metagraph graph annotation.
            rules_in.append(i.name)
        elif x in exclude:
            rules_ex.append(i.name)
        else:
            print(f'No assignment for genome {str(i)}')
        # label = i.parent.__str__().split('/')[-1]
        
        # out.write(f'{ID},{label},{fp}\n')
        out.write(f'{ID},{fp}\n')
        # if label == 'include':
        #     i_delta.append(i.name)
        # else:
        #     o_delta.append(i.name)


# TODO: Create input from 2 folders or the GTDB taxonomy, from this generate
# delta.
delta = {
    'groups': [
        {
            'shared_labels': {
                'in': [],
                'out': [],
            },
            'experiments': [
                {
                    'name': 'legions',
                    'in_min_fraction': 1.0,
                    'out_max_fraction': 0,
                    'in': rules_in,
                    'out': rules_ex,
                }
            ]
        }
    ]
}


with open('delta.json', 'w+') as out:
    json.dump(delta, out, indent=4)

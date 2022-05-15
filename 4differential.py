import pandas as pd


in_, out = [], []
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

        if d['g'] == 'Legionella':
            if d['s'] == 'Legionella pneumophila':
                in_.append(ID)
            else:
                out.append(ID)


i_delta, o_delta = [], []
df = pd.read_csv('input.csv', header=None)
for i in df.itertuples():
    for j in in_:
        if j in i[2]:
            f = i[2].split('/')[-1]
            i_delta.append(f)

    for j in out:
        if j in i[2]:
            f = i[2].split('/')[-1]
            o_delta.append(f)

assert len(in_) == len(i_delta)
assert len(out) == len(o_delta)


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
                    'in': i_delta,
                    'out': o_delta,
                }
            ]
        }
    ]
}


import json
with open('delta.json', 'w+') as out:
    json.dump(delta, out, indent=4)

'''
docker build -n metagraph .

./metagraph assemble -v <GRAPH_DIR>/graph.dbg \
                        --unitigs \
                        -a <GRAPH_DIR>/annotation.column.annodbg \
                        --diff-assembly-rules diff_assembly_rules.json \
                        -o diff_assembled.fa


'''



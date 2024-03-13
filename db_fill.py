import pandas as pd


def rename_func(s):
    return s.replace('(', '').replace(')', '').replace(' ', '_').lower()


################################## STAGES ################################

stages = [ '0', '1', '2', '3a', '3b', '4a', '4b', '5', '7', '10a', '10b' ]

rruff_db = pd.read_csv('data/rruff_database_2024_03_13.csv',
                       #index_col='mineral_name',
                       dtype=str,
                       na_filter=False
                      ).transpose()

rruff_db_stages = {
    s: pd.read_csv(f'data/rruff_database_2021_12_25_S{s}.csv',
                   #index_col='mineral_name',
                   dtype=str,
                   na_filter=False
                  ).transpose()
    for s in stages
}

stages_where_minerals_appeared = { mineral: [] for mineral in rruff_db.keys() }
for mineral in rruff_db.keys():
    for stage in stages:
        if mineral in rruff_db_stages[stage].keys():
            stages_where_minerals_appeared[mineral].append(stage)

rruff_db.loc['Stages'] = stages_where_minerals_appeared.values()

################################## PGMs #################################

pgm = pd.read_csv('data/pgm_ima_2024_03_07.csv',
                  index_col='AA-Mineral Name',
                  dtype=str,
                 ).transpose().fillna(0).replace('1', 1)

def p(mineral, mode):
    try:
        return pgm[mineral][mode]
    except:
        return None

minerals = pgm.columns[1:]
modes = pgm.transpose().columns[:-4]

mineral_pgms = { mineral:
                [ mode for mode in modes if p(mineral, mode) == 1 ]
                for mineral in minerals
               }


# putting data in db appropriate form

rruff_db.rename(rename_func, inplace=True)
print(rruff_db)
d = rruff_db.transpose().to_dict('records')
for record in d:
    tmp = record['country_of_type_locality'].replace(' /', '/').replace('/ ', '/').replace('/ ', '/')

    record['chemistry_elements'] = record['chemistry_elements'].split(' ')
    record['rruff_ids'] = record['rruff_ids'].split(' ')
    record['ima_status'] = record['ima_status'].split('|')
    record['space_groups'] = record['space_groups'].split('|')
    record['country_of_type_locality'] = tmp.split('/')
    record['crystal_systems'] = record['crystal_systems'].replace(', ', '|').split('|')
    record['valence_elements'] = record['valence_elements'].split(' ')
    record['database_id'] = int(record['database_id'])
    record['year_first_published'] = int(record['year_first_published'])
    try:
        record['pgm'] = mineral_pgms[record['mineral_name']]
    except:
        pass

print('records obtained')

rruff_db = pd.DataFrame(d)
print(rruff_db)
rruff_db.to_csv('data/rruff_db_tot_filtered.csv', index=False)
print('Done.')

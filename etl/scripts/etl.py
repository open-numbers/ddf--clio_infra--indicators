# -*- coding: utf-8 -*-

"""main etl script"""

import os
import os.path as osp
import pandas as pd

from ddf_utils.str import to_concept_id
from ddf_utils.transformer import extract_concepts
from ddf_utils.datapackage import get_datapackage, dump_json


datapage = 'Data Long Format'
metadatapage = 'Metadata'
out_path = '../../'
source_path = '../source'


def process_file(fn):
    data = pd.read_excel(fn, sheet_name=datapage)
    metadata = pd.read_excel(fn, sheet_name=metadatapage)
    geo_data = data[['ccode', 'country.name']].drop_duplicates()
    indicator_data = data[['country.name', 'year', 'value']].copy()

    concept_name = osp.splitext(osp.basename(fn))[0]
    concept_id = to_concept_id(concept_name)

    metadata = metadata.set_index('Description').T.reset_index(drop=True)
    metadata['concept'] = concept_id
    metadata['concept_type'] = 'measure'
    metadata['name'] = concept_name
    metadata = metadata.set_index('concept')

    indicator_data.columns = ['country', 'year', concept_id]
    indicator_data['country'] = indicator_data['country'].map(to_concept_id)
    indicator_data = indicator_data.set_index(['country', 'year'])

    if indicator_data.index.has_duplicates:
        print(f'duplicated index found in {fn}')
        print(indicator_data.index[indicator_data.index.duplicated()])
        indicator_data = indicator_data[~indicator_data.index.duplicated(keep='first')]

    geo_data.columns = ['ccode', 'name']
    geo_data['country'] = geo_data.name.map(to_concept_id)
    geo_data = geo_data.set_index('country')

    return (metadata, geo_data, indicator_data)


if __name__ == '__main__':

    concepts = []
    geos = []

    for fn in os.listdir(source_path):
        # process all file, save datapoints and get geos and concepts from each file.
        if fn.endswith('xlsx') or fn.endswith('xls'):
            cdf, geo, ind = process_file(osp.join('../source', fn))
            ind.to_csv(osp.join(out_path,
                                f'ddf--datapoints--{cdf.index[0]}--by--country--year.csv'))
            concepts.append(cdf)
            geos.append(geo)

    # create concept dataframe and country dataframe
    cdf_full = pd.concat(concepts)
    cdf_full = cdf_full[['name', 'concept_type', 'Downloaded from', 'Text Citation',
                         'XML Citation', 'RIS Citation', 'BIB Citation']]
    cdf_full.columns = cdf_full.columns.map(to_concept_id)
    geo_full = pd.concat(geos)
    geo_full = geo_full.drop_duplicates(subset=['name'])
    geo_full['ccode'] = geo_full['ccode'].map(lambda x: str(int(x)) if not pd.isnull(x) else '')
    geo_full.to_csv(osp.join(out_path, 'ddf--entities--country.csv'))

    # extract discrete concepts
    discrete = extract_concepts([cdf_full, geo_full.reset_index()])
    discrete = discrete.set_index('concept')
    discrete.loc['country', 'concept_type'] = 'entity_domain'
    discrete.loc['year', 'concept_type'] = 'time'
    discrete.loc['ccode', 'concept_type'] = 'string'
    discrete['name'] = discrete.index

    cdf_full = cdf_full.append(discrete).drop_duplicates(subset=['name'])
    cdf_full.to_csv(osp.join(out_path, 'ddf--concepts.csv'))

    dump_json(os.path.join(out_path, 'datapackage.json'), get_datapackage(out_path))

    print('Done.')

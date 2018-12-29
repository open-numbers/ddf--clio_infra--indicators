# -*- coding: utf-8 -*-


from ddf_utils.factory import ClioInfraLoader


datapage = 'Data Long Format'
metadatapage = 'Metadata'
out_path = '../../'
source_path = '../source'


if __name__ == '__main__':
    # download source files
    print("updating source files...")
    ClioInfraLoader().bulk_download(source_path, data_type='dataset')

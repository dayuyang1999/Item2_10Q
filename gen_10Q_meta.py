import dask
import pandas as pd
import re
#from tqdm import tqdm_notebook as tqdm

# load_master_idx
def load_idx(data_fp, year):
    '''

    :param data_fp:
    :param year: year of original idx

    :return: idx
    '''
    pd_10x = pd.read_csv(f"{data_fp}/LM_10X_Summaries_{year}.csv", parse_dates=['FILING_DATE', 'FYE'])
    return pd_10x

# add company name
def preprocess(data_fp='', pd_10x=None, y_begin=2016, y_end=2018):
    '''

    :param data_fp: if py is in master directory, stay ''
    :param pd_10x: output of load_idx
    :param y_begin:
    :param y_end:

    :return:
    '''

    pd_10Q = pd_10x[(pd_10x.FORM_TYPE == '10-Q') & (pd_10x.FILING_DATE.dt.year >= y_begin) & (pd_10x.FILING_DATE.dt.year <= y_end)].copy()
    print('filing number count_by_year',
          '\n',
          pd_10Q.FILING_DATE.dt.year.value_counts().sort_index())
    print("------------------------------------- creating master file......")
    # change dir
    pd_10Q.loc[:, 'FILE_NAME'] = pd_10Q.FILE_NAME.apply(lambda x: x.replace('D:/Edgar/', f'{data_fp}/'))
    # change to macos address
    pd_10Q.loc[:, 'FILE_NAME'] = pd_10Q.FILE_NAME.apply(lambda x: x.replace('\\', '/'))
    # keep vars we need
    pd_10Q = pd_10Q[['CIK', 'FILING_DATE', 'FYE', 'SIC', 'FFInd', 'FILE_NAME', 'N_Words', 'N_Unique_Words']].copy()
    pd_10Q['org_index'] = pd_10Q.index.values
    pd_10Q.reset_index(drop=True, inplace=True)
    return pd_10Q

def add_name(pd_10Q):
    rule1 = re.compile('<SEC-Header>(.*)</SEC-Header>', re.I | re.DOTALL)
    rule2 = re.compile('^ *?company conformed name:(.*?)$', re.I | re.M)

    @dask.delayed
    def get_company_name(i, pd_10Q):
        '''

        :param i: row of pd_10Q
        :param pd_10Q: output of preprocess

        logistic: finding name after text "company conformed name:" in 10Q form

        :return: company name
        '''
        company_name = ""
        file_fp = f"{pd_10Q.loc[i, 'FILE_NAME']}"
        with open(file_fp) as f:
            filing_str = f.read()
            r1 = rule1.search(filing_str)
            if r1:
                filing_str = rule1.search(filing_str).group(1).replace('\t', '')
            r = rule2.search(filing_str)
            if r:
                company_name = r.group(1)
        return i, company_name

    # n_row = 10
    n_row = pd_10Q.shape[0]
    company_names = [get_company_name(i, pd_10Q) for i in range(n_row)]
    results = dask.compute(company_names, num_workers=16)[0]

    _index, _data = zip(*results)
    company_names = pd.Series(data=_data, index=_index)
    company_names.sort_index(inplace=True)

    pd_10Q['company_name'] = company_names

    return pd_10Q



# Get file hash to identify firms which file exactly the same contents.
def get_file_hash(_str):
    re_file_hash = re.compile('edgar_data_\d+_(.+)\.txt')
    r = re_file_hash.search(_str)
    if r:
        return r.group(1)
    else:
        return ''

def __main__():
    latest = 2018 # Summary file version
    years = [2007, 2018] # start and end
    data_fp = '/Users/dylan/Downloads/10Q_item2' # do not add / at end


    pd_10x = load_idx(data_fp, latest)  # the latest version of super master idx (contains from 1994 to latest year, all 10-? documents)
    # get 10q from y_begin to y_end
    pd_10Q = preprocess(data_fp=data_fp, pd_10x=pd_10x, y_begin=years[0], y_end=years[1]) # includes y_end
    pd_10Q_added = add_name(pd_10Q)
    pd_10Q['file_hash'] = pd_10Q.FILE_NAME.apply(get_file_hash)
    file_count = pd_10Q.file_hash.value_counts().reset_index()
    file_count = file_count.rename({'file_hash': 'file_hash_count', 'index': 'file_hash'}, axis=1)
    pd_10Q = pd_10Q.merge(file_count, on='file_hash', how='left')
    print('------------------------------ master file create finished')
    # info of all 10-q from y_s to y_e (csv file)
    pd_10Q.to_csv(f"{data_fp}/10Q_master_v1.csv")

__main__()


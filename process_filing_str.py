import pandas as pd
import re
#import webbrowser
import numpy as np
from tqdm import tqdm
#import pprint
import pickle
from datetime import datetime
import os

#import dask
#from dask.diagnostics import ProgressBar





# save temp results
re_m_exbibit = re.compile('<N_Exhibits>(\d+)</N_Exhibits>')
re_f_exbibit = re.compile('<EX-.*')
re_f_exbibit_d = re.compile('<EX-(\d+\.?\d*).*')

def get_filing_str_l(year):
    print("n_row:", (pd_10Q.filing_date.dt.year == year).sum())
    filing_str_l = []
    pd_idx_l = []
    idx_d = {}
    for i, (pd_idx, row) in tqdm(enumerate(pd_10Q[pd_10Q.filing_date.dt.year == year].iterrows()),
                                 total=len(pd_10Q[pd_10Q.filing_date.dt.year == year])):
        # read file
        one_10q_fname = f"{data_fp}/{row['file_name']}"
        with open(one_10q_fname, 'r') as f:
            doc_str = f.read()
        # remove exibits
        # by construction, we know every doc has a N_Exhibits tag
        # so the search should not return none
        n_exb = int(re_m_exbibit.search(doc_str).group(1))
        if n_exb > 0:
            exb_l = list(re_f_exbibit.finditer(doc_str))
            assert len(exb_l) == n_exb
            doc_str = doc_str[:exb_l[0].start()]
        # remove header
        doc_str = doc_str[(doc_str.index('</Header>') + 9):]
        # replace tab with space
        doc_str = doc_str.replace('\t', ' ')
        # output
        filing_str_l.append(doc_str)
        pd_idx_l.append(pd_idx)
        idx_d[pd_idx] = i

    return filing_str_l, pd_idx_l, idx_d

def apply_sub_fixs(str_l, fix_l, squeezed=False):
    if squeezed:
        fix_l = [fix_l]
    for i, doc_str in tqdm(enumerate(str_l), total=len(str_l)):
        for re_obj, repls in fix_l:
            doc_str = re_obj.sub(repls, doc_str)
        str_l[i] = doc_str
    return str_l
spell_fixes = [
    # the first two rules keep capitalization of letters
    (re.compile('(i)(\s*?t\s*?e\s*?m)', re.I|re.M), '\g<1>tem'),  # i t e       m
    (re.compile('(i)(tem\s+?s)([^\w\-])', re.I|re.M), '\g<1>tems\g<3>'),  # item       s?
    (re.compile('(b?\s{,4}?u\s{,4}?s\s{,4}?i\s{,4}?n\s{,4}?e\s{,4}?s\s{,4}?s)(\W)', re.I|re.M), 'business\g<2>'),
]     #  have no idea what is this. {0,4} btw 0,4

def save_tmp(data_fp, year_start, year_end):
    '''
    using both:
        read: get_filing_str_l
        normalize: apply_sub_fixs
    save to tmp folder
    '''

    if os.path.exists(f"{data_fp}/tmp") == False:
        os.makedirs(f"{data_fp}/tmp")
    for y in tqdm(range(year_start, year_end)):
        print('-------- processing: ', y)  # tqdm 1
        filing_str_l, pd_idx_l, idx_d = get_filing_str_l(y)  # tqdm 2
        filing_str_l = apply_sub_fixs(filing_str_l, spell_fixes)  # tqdm 3
        with open(f"{data_fp}/tmp/filing_{y}.data", 'wb') as f:
            pickle.dump([filing_str_l, pd_idx_l, idx_d], f)

        tmp = pd_10Q.iloc[np.random.choice(pd_idx_l)]  # randomly listed

        assert tmp.filing_date.year == y



years = [2007, 2018]   # includes end year
data_fp = '/Users/dylan/Downloads/10Q_item2'  # do not add "/" at end


pd_10Q = pd.read_csv(f'{data_fp}/10Q_master_v1.csv')
pd_10Q.columns = list(map(lambda _s: _s.lower(), pd_10Q.columns))
pd_10Q.file_name = pd_10Q.file_name.apply(lambda str: str.replace(f'{data_fp}/', ""))
pd_10Q.filing_date = pd_10Q.filing_date.apply(lambda str: datetime.strptime(str, '%Y-%m-%d'))
pd_10Q = pd_10Q.rename(columns={pd_10Q.columns[0]:'pd_10q_idx'})



save_tmp(data_fp, years[0], years[1]+1)
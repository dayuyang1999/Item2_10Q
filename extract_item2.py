import pandas as pd
import re
import webbrowser
import numpy as np
from tqdm import tqdm
#import pprint
import pickle
import dask
from dask.diagnostics import ProgressBar
import os
from datetime import datetime



# Ensemble edgar link from file name.
def get_edgar_link(fname, open_in_browser=True):
    base_url = 'https://www.sec.gov/Archives/edgar/data/'
    re_result = re.search('edgar_data_(\d+)_(.+)_(\d+).txt', fname)
    fname_major = re_result.group(2)
    edgar_link = base_url + re_result.group(1) + '/' + re.sub('\-|_', '', fname_major) + '/' + fname_major + '-index.htm'
    if open_in_browser:
        webbrowser.open(edgar_link)
    return edgar_link


## extract func
# find start of item 2

@dask.delayed
def find_item2_start(i, filing_str_l):
    '''
    finding the start of item 2 in ith document

    no return,
    update span_pd[i,0] and status_pd[i,0]
    '''
    target = filing_str_l[i]

    # default using pattern 1
    finds_l = list(pattern1.finditer(target))

    if len(finds_l) > 1:  # ideally, len should be 2; some times was split to multiple
        if finds_l[1].span(0)[0] > 8000:
            span_pd.loc[i, 0] = finds_l[1].span(0)  # begin with "item"
            status_pd.loc[i, 0] = 1
        elif finds_l[1].span(0)[0] > 2500:
            span_pd.loc[i, 0] = finds_l[1].span(0)  # update for further check
            status_pd.loc[i, 0] = 2
            #print(f"WARNING: hybrid, more than 2 found, location of item 2 < 8000 in filing_str_l[{i}]")
        else:
            status_pd.loc[i, 0] = 3  # do not update
            #print(f"FAIL: hybrid, more than 2 found, location of item 2 < 2500 in filing_str_l[{i}]")

    elif len(finds_l) == 1:
        if finds_l[0].span(0)[0] > 8000:
            span_pd.loc[i, 0] = finds_l[0].span(0)
            status_pd.loc[i, 0] = 1
        elif finds_l[0].span(0)[0] > 2500:
            span_pd.loc[i, 0] = finds_l[0].span(0)  # update anyway
            status_pd.loc[i, 0] = 2
            #print(f"WARNING: hybrid, only 1 found, location of item 2 < 8000, in filing_str_l[{i}]")
        else:
            status_pd.loc[i, 0] = 3  # do not update

            #print(f"FAIL: hybrid, only 1 found, location of item 2 < 2500 in filing_str_l[{i}]")
    else:
        # using literally match
        finds_l = list(pattern2.finditer(target))
        if len(finds_l) >= 1:

            if finds_l[0].span(0)[0] > 8000:
                span_pd.loc[i, 0] = finds_l[0].span(0)  # get the 1st
                status_pd.loc[i, 0] = 1
            elif finds_l[0].span(0)[0] > 2500:
                span_pd.loc[i, 0] = finds_l[0].span(0)  # update anyway
                status_pd.loc[i, 0] = 2
                #print(f"WARNING: literally, location of letter < 8000 in filing_str_l[{i}]")
            else:
                status_pd.loc[i, 0] = 3
                #print(f"FAIL: literally, location of letter < 2500 in filing_str_l[{i}]")
        else:
            status_pd.loc[i, 0] = -1
            #print(f'FAIL: no "item2" was found in filing_str_l[{i}]')







@dask.delayed
def find_item2_end(i, filing_str_l):
    target = filing_str_l[i]

    if status_pd.loc[i, 0] not in [1, 2]:
        print(" ")
    else:
        for n_rule, rule in enumerate(rules):
            finds_l = list(rule.finditer(target))
            if len(finds_l) >= 1:
                for k, mo in enumerate(finds_l):  # mo means match object
                    # print(mo.span(0)[0] ,"\n", span_pd.loc[i, 0][0])
                    # print(k,mo.span(0)[0],span_pd.loc[i, 0][0])
                    if (mo.span(0)[0] > span_pd.loc[i, 0][0]):
                        span_pd.loc[i, 1] = mo.span(0)
                        status_pd.loc[i, 1] = 1  # success
                        return
                    elif (n_rule == len(rules) - 1) & (k == len(finds_l) - 1) & (
                            mo.span(0)[0] <= span_pd.loc[i, 0][0]):  # last mo
                        status_pd.loc[i, 1] = 2  # further checking
                        #print(f'all match fail in filing_str_l[{i}]')
                        break  # try next rule
            else:
                continue  # try next rule

## save item2s
def save_item2(fp, pd_10Q, filing_str_l):
    if os.path.exists(f'{data_fp}/Extracted/{which_year}/'):
        pass
    else:
        os.makedirs(f'{data_fp}/Extracted/{which_year}/')
    pd_10Q.to_csv(meta_fname, index=False)
    print("saving item2 txt files .......")
    subpd = pd_10Q[pd_10Q.filing_date.dt.year == which_year].reset_index()
    idx_start = subpd.loc[0, 'pd_10q_idx']
    idx_end = subpd.shape[0] + idx_start
    for i in tqdm(range(idx_start, idx_end)):  # number of 10q for a year
        j = idx_d[pd_10Q["pd_10q_idx"][i]]
        if (status_pd.iloc[j, 0] in [1, 2]) & (status_pd.iloc[j, 1] in [1, 2]):  # start and end both success
            file_hash = pd_10Q['file_hash'][i]
            start = span_pd.iloc[j, 0][0]
            end = span_pd.iloc[j, 1][0]
            doc_str = filing_str_l[j][start:end]
            doc_str = re.sub('\s+', ' ', doc_str)
            if len(doc_str) > 200:
                with open(it2_file_init + f"item2_{file_hash}.txt", "w") as f:
                    f.write(doc_str)




# define patterns -------------
pattern1 = re.compile("(^\s*?|\s{4,}|\\n+)((i)(tem)\s*?n?o?\s*?\.?\s*s?\s*?)(2)\s*?([,: .\n\-\s]+)([a-zA-Z\s\,\.]{0,50})\s{1,}(((Management|trustee)([a-zA-Z\s\']{10,90}))|((discussion)([a-zA-Z\s]{10,80})))(\s{4,}|\n+|\.+)", re.I | re.M)
pattern1_1 = re.compile("item", re.I|re.M)
# pure literal, match "management discussion and analysis of financial  CONDITION AND RESULTS OF OPERATIONS "
# case 1: do not write "item 2" before title
# case 2: item 1, 2, 3, and 4....like this
pattern2 = re.compile("(^\s*?|\s{4,}|\\n+)(Management|trustee)([a-zA-Z\s\']{1,10})\s*?\n*?(Discussion)(\s+|\n+)(and)(\s+|\n+)(analysis)(\s+|\n+)(of)(\s+|\n+)(financial)(\s+|\n+)(condition)(\s+|\n+)(and)(\s+|\n+)(results)(\s+|\n+)(of)(\s+|\n+)(operations)(\s{2,}|\n+|\.+)", re.I|re.M)
pattern2_1 = re.compile("Management|trustee", re.I|re.M)


# find the end of item 2
rule1 = re.compile("(^\s*?|\s{2,}|\\n+)((i)(tem)\s*?n?o?\s*?\.?\s*s?\s*?)(3)\s*?([,: .\n\-])([\s\-]*?)([a-zA-Z\s\,\.]{0,50})\s{1,}(((quantitative)([a-zA-Z\s\']{10,90}))|((qualitative)([a-zA-Z\s]{10,80})))(\s{4,}|\n+|\.+)", re.I|re.M)
rule2 = re.compile("(^\s*?|\s{2,}|\\n+)((i)(tem)\s*?n?o?\s*?\.?\s*s?\s*?)(4|3|4T)\s*?([,: .\n\-])([a-zA-Z\s\,\.]{0,50})([\s|\-]*?)(((controls)([a-zA-Z\s]{10,20}))|((procedures)))(\s{4,}|\n+|\.+)", re.I|re.M)
rule3 = re.compile("(^\s*?|\s{2,}|\\n+)((part)\s*?(ii))\s*?([,: .\n\-])([\s|\-]*?)(other)\s*?([,: .\n\-])(information)(\s{4,}|\n+|\.+)", re.I|re.M)
# pure letter, item 3
rule4a = re.compile("(^\s*?|\s{4,}|\\n+)(quantitative)(\s+|\n+)(and)(\s+|\n+)(qualitative)(\s+|\n+)(disclosures)(\s+|\n+)(about)(\s+|\n+)(market)(\s+|\n+)(risk)(\s{2,}|\n+|\.+)", re.I|re.M)
# item 4
rule4b = re.compile("(^\s*?|\s{4,}|\\n+)(controls)(\s+|\n+)(and)(\s+|\n+)(procedures)(\s{2,}|\n+|\.+)", re.I|re.M)
rules = [rule1, rule2, rule3, rule4a, rule4b]





'''
set 3 params:
which_year: extract item 2 from all 10-Q reports of this year
cpu_core: n of cores of your cpu
data_fp = where 10Q_master_v1.csv is
'''
####################################### params you should change ------------------
years = [2007, 2018]  # start year and end year; includes end
cpu_core = 6
data_fp = '/Users/dylan/Downloads/10Q_item2' # do not add "/" at end
test = 10 # for testing program, if extract all item2, remain None.

#######################################





# load master table -----------
# processing
pd_10Q = pd.read_csv(f'{data_fp}/10Q_master_v1.csv')
pd_10Q.columns = list(map(lambda _s: _s.lower(), pd_10Q.columns))
pd_10Q.file_name = pd_10Q.file_name.apply(lambda str: str.replace(f'{data_fp}/', ""))
pd_10Q.filing_date = pd_10Q.filing_date.apply(lambda str: datetime.strptime(str, '%Y-%m-%d'))
pd_10Q = pd_10Q.rename(columns={pd_10Q.columns[0]:'pd_10q_idx'})



for year in range(years[0], years[1]+1):
    print(f"extracting item 2 of year {year}")
    which_year = year
    # read files ------------------
    s1_fname = data_fp + f'/tmp/filing_{year}.data'
    with open(s1_fname, 'rb') as f:
        filing_str_l, pd_idx_l, idx_d = pickle.load(f)

    n_docs = len(filing_str_l)

    # refresh
    span_pd = pd.DataFrame.from_dict({i: [(0, 0), (0, 0)] for i in range(n_docs)}, orient='index')
    status_pd = pd.DataFrame.from_dict({i: [0, 0] for i in range(n_docs)}, orient='index')

    print("extracting start of item 2......")
    update_tasks_s = [find_item2_start(i, filing_str_l) for i in tqdm(range(len(filing_str_l[:test])))]

    # finding start

    with ProgressBar():
        dask.compute(update_tasks_s, num_workers = cpu_core*2)

    print('no update recorded:          ', sum(status_pd[0] == 0), '\n'
                                                                   'succeeded:                   ',
          sum(status_pd[0] == 1), '\n',
          'updated,          loc < 8000 :', sum(status_pd[0] == 2), '\n',
          'marked as failed, loc < 2500: ', sum(status_pd[0] == 3), '\n',
          'confirmed no item 2:          ', sum(status_pd[0] == -1), '\n',
          sep='')

    # finding end
    print("extracting end of item 2......")
    update_tasks_e = [find_item2_end(i, filing_str_l) for i in range(len(filing_str_l[:test]))]
    with ProgressBar():
        dask.compute(update_tasks_e, num_workers = cpu_core*2)


    # export results ---------------------
    status_pd['pd_idx'] = pd_idx_l
    # get filing url
    pd_10Q['edgar_url'] = ""
    for i, row in pd_10Q.iterrows():
        pd_10Q.loc[i, 'edgar_url'] = get_edgar_link(row['file_name'], False)
    meta_fname = f'{data_fp}/Extracted/{which_year}/Extracted_Meta.csv'
    it2_file_init = f'{data_fp}/Extracted/{which_year}/'
    save_item2(data_fp, pd_10Q, filing_str_l)
    print(f"---------------------------------------- year {which_year} finished! -------------------------------------------")



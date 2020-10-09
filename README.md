Goal: Parse Item2 from 10-Q

> based on Bill McDonald's work. 

# Data Description

## Folder Structure
Professor McDonald kindly provides all the parsed 10K document on his website [Notre Dame Software Repository for Accounting and Finance (SRAF)](https://sraf.nd.edu/). See page [Stage One 10-X Parse Data](https://sraf.nd.edu/data/stage-one-10-x-parse-data/). 

**Data Version: Updated 201901**

Once all are unzipped into the **root directory** ("D:\MyData\EDGAR"), the resulted file tree structure is

```
root
-- 10-X_C
---- [4-digit year]
------ QTR[1-digit quarter number]
--------- [date]_10-[form type]_[file name].txt
```


The total size of folder 10-X_C is about 140GB. In addition, two folders, `10K_ITEM2` and `tmp` are created in the root directory, to store processed documents.
## FIling types
Every year-quarter sub folder in `10-X_C` contains all the 10-X filings. Available filing types are


```
'10-Q', '10-K-A', '10-K', '10-Q-A', '10-K405', '10KSB', '10-KT',
'10-QT-A', '10-QT', '10QSB', '10-K405-A', '10QSB-A', '10KT405',
'10KSB-A', '10KSB40', '10KT405-A', '10KSB40-A', '10-KT-A',
'10-QSB-A', '10-QSB', '10-KSB', '10-KSB-A'
```

We only focus on "10-Q" forms, which includes:
```
['10-Q', '10QSB', '10-QSB']
```

## Master file
download the master data file from  [Loughran and McDonald 10X File Summaries](https://sraf.nd.edu/textual-analysis/resources/#LM_10X_Summaries). The file is downloaded as `LM_10X_Summaries_2018.csv` and placed in the root directory. This file might be updated and renamed by the authors to include data of more recent years.

Note that although 10-X reports are available up to year 2019, the meta data for them is only available up to year 2018. We need meta data to know CIK, FILING_DATE, FORM_TYPE, and other information of a plain text file. For this reason, I only process documents up to year 2016.

# Processing
## New Master
Notebook `gen_10Q_meta.ipynb`(as well as `.py`) takes `[root]/LM_10X_Summaries_2018.csv` and all the raw 10-Q reports as input. It will extract the rows for 10-Q reports from the original data frame. Then it will scan all the 10-Q reports to add some additional meta information. The final results will be a pandas data frame saved as `[root]/10Q_master_v1`.
## Filing_str_l processing
run `process_filing_str.py`; will save `year_end` - `year_start` +1 files in /tmp/

```
params you should change:
 + data_fp
 + years

```

normalize filing_str_l from `year_start` to `year_end` for later use



# Extract Item2
Notebook `extract_item2.ipynb` or`.py` takes  `[root]/10Q_master_v1` and all the raw 10-Q reports as input, and then extracts the desired Item 2 sections (in part I of 10Q) for a single year. Suppose the year is `xxxx`.  3 files will be generated.
```
params you should change:
    years  # start year and end year
    cpu_core 
    data_fp 


```

+ `[root]/tmp/[filing_xxxx.data]` stores the 10-Q reports for the single year where irrelevant content will be discarded such as headers, exhibits. Text quality is improved by fixing the spells of several important words and phrases.

+ `[root]/Extracted/year/Extracted_Meta.csv` stores sub year meta data extracted from `10Q_master_v1`. Compared with `10Q_master_v1`, it only keep first filings for a firm if the firm reports multiple filings in the year. For convenience, it also include a new column indicating original EDGAR url for the 10-Q reports, which is useful for case study.

+ `[root]/Extracted/year/item2_filehash.txt` stores the extracted Item 2 sections from the 10-Q reports. One txt file for a report


# Step by Step guidance 

## prepare data
1. create a data_fp in your divice
2. download data from [10-x stage one data](https://drive.google.com/drive/folders/1tZP9A0hrAj8ptNP3VE9weYZ3WDn9jHic)
and unzip as folders to your data_fp
3. arrange 10-X_C data to fit the dir structure at top..
4. Download latest [master file](https://sraf.nd.edu/textual-analysis/resources/#LM_10X_Summaries) to data_fp

```
After that, your data_fp shoule like this(2 items in data_fp):

-- data_fp
    -- 10-X_C
    -- LM_10X_....csv
```
## run gen_10q_meta.py
1. change params:
    + latest = 2018 (downloaded data version, also as postfix of name of file)
    + years = [?, ?] # start and end year
    + data_fp = e.g. '/Users/dylan/Downloads/10Q_item2'
2. run
3. a new file will shown on your data_fp 
    + 10Q_master_v1.csv(which contains all 10-Q info from year_start to year_end)

## run process_filing_str.py
1. change params:
    + years = [?, ?] # could be a subset of years you set in `gen_10q_meta`
    + data_fp = must be same as before, e.g. '/Users/dylan/Downloads/10Q_item2'

2. run
3. a new folder will show on your data_fp
    + tmp
        + filing_year1
        + filing_year2
        + ...
## extract_item2.py
1. change params:
    + years = [?, ?]  # could be subset...
    + cpu_core = ?  # n_cores of your cpu. 4, 6, 8 ...   
    + data_fp = must be the same...e.g. '/Users/dylan/Downloads/10Q_item2'
    + test = None; only for program testing, you could set as 30 to avoid waiting for too long. but keep "None" if run all item2 
2. run
3. a few folder will shown in data_fp
    + Extracted
        + year
            + item2_filehash.txt 
            + Extracted_Meta.csv (index and other info)


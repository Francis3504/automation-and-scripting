import pandas as pd
import difflib
from pathlib import Path


EXPECTED_COLUMNS=["invoice_id","date","store_id","product","quantity","unit_price","total","regions"]
HEADER_MAP={"invoiceid":"invoice_id","storeid":"store_id","store":"store_id","unitprice":"unit_price"}

def load_csv_files(folder:Path):
  for file in folder.iterdir():
    if file.suffix.lower()==".csv" and file.is_file():
        yield file

def read_csv_files(files:Path):
    try:
       return pd.read_csv(files)
    except Exception:
        return None
    
def normalize_headers(df:pd.DataFrame):
    df=df.copy()

    news_cols={}
    for col in df.columns:
        cleaned_header=col.strip().lower().replace(" ","")
        
        if cleaned_header in HEADER_MAP:
            news_cols[cleaned_header]=HEADER_MAP[cleaned_header]

        elif cleaned_header in EXPECTED_COLUMNS:
            news_cols[cleaned_header]=cleaned_header
            
        else:
            match=resolve_column(cleaned_header,EXPECTED_COLUMNS)
            if match:
                news_cols[col]=match
            else:
                news_cols[col]=None

    df=df.rename(columns={k:v for k,v in news_cols.items() if v})

    df=df[[c for c in df.columns if c in EXPECTED_COLUMNS]]
    
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col]=pd.NA

    return df     

def resolve_column(col:str,expected_cols:list,threshold:float=0.85)->str:
    matches=difflib.get_close_matches(col,expected_cols,n=1,cutoff=threshold)
    return matches[0] if matches else None



def clean_data(df:pd.DataFrame):
    df=df.copy()
    df=df.dropna(subset=["invoice_id","date"])
    df["quantity"]=pd.to_numeric(df["quantity"],errors="coerce")
    df["unit_price"]=pd.to_numeric(df["unit_price"],errors="coerce")
    df["total"]=pd.to_numeric(df["total"],errors="coerce")
    return df

def load_all_data(folder:Path):
    frames=[]
    for file in load_csv_files(folder):
        df=read_csv_files(file)
        if df is None:
            continue

        df=normalize_headers(df)
        df=clean_data(df)
        frames.append(df)

    return pd.concat(frames,ignore_index=True)


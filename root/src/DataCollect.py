# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 22:30:28 2025

@author: Diego
"""

import os
import pandas as pd

class DataManager:
    
    def __init__(self) -> None:
        
        
        self.dir       = os.path.dirname(os.path.abspath(__file__))  
        self.root_path = os.path.abspath(
            os.path.join(os.path.abspath(
                os.path.join(self.dir, os.pardir)), os.pardir))
        
        self.data_path      = os.path.join(self.root_path, "data")
        self.raw_data_path  = os.path.join(self.data_path, "RawData")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        if os.path.exists(self.raw_data_path) == False: os.makedirs(self.raw_data_path)
        
        self.bbg_data_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\data"
        
        self.bbg_xlsx_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\root\BBGTickers.xlsx"
        if os.path.exists(self.bbg_xlsx_path) == False: 
            self.bbg_xlsx_path = r"/Users/diegoalvarez/Desktop/BBGData/root/BBGTickers.xlsx"
        
        self.bbg_fut_path   = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\root\fut_tickers.xlsx"
        if os.path.exists(self.bbg_fut_path) == False: 
            self.bbg_fut_path = r"/Users/diegoalvarez/Desktop/BBGFuturesManager/root/fut_tickers.xlsx"
        
        self.bbg_front_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\PXFront"
        self.bbg_deliv_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\BondDeliverableRisk"
        
        self.df_fut_tickers = (pd.read_excel(
            io = self.bbg_fut_path, sheet_name = "px"))

    def _get_tsy_rtn(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_out = (df.sort_values(
            "date").
            assign(
                PX_rtn  = lambda x: x.PX_LAST.pct_change(),
                PX_diff = lambda x: x.PX_LAST.diff(),
                PX_bps  = lambda x: x.PX_diff / x.PX_dur).
            dropna())
        
        return df_out

    def get_tsy_fut(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_data_path, "TreasuryFutures.parquet")
        try:
            
            if verbose == True: print("Seaching for Treasury futures")
            df_fut = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find it, collecting") 
            raw_tickers = (self.df_fut_tickers.assign(
                second = lambda x: x.name.str.split(" ").str[-2]).
                query("second == 'Treasury'").
                contract.
                to_list())
            
            deliv_paths = ([
                os.path.join(self.bbg_deliv_path, path_ + ".parquet") 
                for path_ in raw_tickers])
            
            df_deliv = (pd.read_parquet(
                path = deliv_paths, engine = "pyarrow").
                pivot(
                    index   = ["date", "security"], 
                    columns = "variable",
                    values  = "value").
                reset_index().
                rename(columns = {
                    "CONVENTIONAL_CTD_FORWARD_FRSK": "PX_dur",
                    "FUT_EQV_CNVX_NOTL"            : "PX_cnvx"}))
            
            fut_paths = ([
                os.path.join(self.bbg_front_path, ticker + ".parquet") 
                for ticker in raw_tickers])
            
            df_fut = (pd.read_parquet(
                path = fut_paths, engine = "pyarrow").
                merge(right = df_deliv, how = "inner", on = ["date", "security"]).
                dropna().
                groupby("security").
                apply(self._get_tsy_rtn).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_fut.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_fut
    
def main() -> None:
    
    DataManager().get_tsy_fut(verbose = True)
    
if __name__ == "__main__": main()
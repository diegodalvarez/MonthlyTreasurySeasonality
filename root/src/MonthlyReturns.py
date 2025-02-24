# -*- coding: utf-8 -*-
"""
Created on Sun Feb 23 22:33:22 2025

@author: Diego
"""

import os
import pandas as pd
from   DataCollect import DataManager

class MonthlyReturns(DataManager):
    
    def __init__(self) -> None:
        
        super().__init__()
        self.monthly_returns = os.path.join(self.data_path, "MonthlyReturns")
        if os.path.exists(self.monthly_returns) == False: os.makedirs(self.monthly_returns)
        
        self.shift = 10
        
    def _get_monthly_rtn(self, df: pd.DataFrame, shifter: int) -> pd.DataFrame: 
        
        if len(df) > 6:
            
            df_tmp = df.sort_values("date")
            
            df_out = (pd.concat([
                df_tmp.head(shifter).assign(
                    group = "front",
                    days  = [i + 1 for i in range(shifter)]),
                df_tmp.tail(shifter).assign(
                    group = "back",
                    days  = [shifter - i for i in range(shifter)])]))
            
            return df_out
            
        else: return None
        
    def get_monthly_rtn(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.monthly_returns, "TreasuryFutures.parquet")
        try:
            
            if verbose == True: print("Seaching for Monthly Seasonality")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find it, collecting") 
            
            df_out = (self.get_tsy_fut().assign(
                security   = lambda x: x.security.str.split(" ").str[0],
                month_date = lambda x: pd.to_datetime(x.date).dt.strftime("%Y-%m"),
                group_var  = lambda x: x.security + "_" + x.month_date).
                groupby("group_var").
                apply(self._get_monthly_rtn, self.shift).
                reset_index(drop = True).
                drop(columns = ["PX_LAST", "group_var"]))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out

def main() -> None:
        
    MonthlyReturns().get_monthly_rtn(verbose = True)

if __name__ == "__main__": main()
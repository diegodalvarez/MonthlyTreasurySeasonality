# Trading Treasuries Month End

This GitHub repo replicates the work from [*Hartley, Jonathan and Schwarz, Krista, Predictable End-of-Month Treasury Returns*](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3440417). The idea is that month end flows from insurance companies -specifically window-dressing creates a month end effect. This notebook tests that claim and corroborates its. The sharpe is considerably significant depending on what day is chosen. The in-sample and out-of-sample returns are matched quite closely. The backtest doesn't really on an formalized model to identify month end effects but rather moving averages. 

All calculations are done for Treasury Futures and done in basis points. 

The returns of picking any window between 1-10 days before month end.
![image](https://github.com/user-attachments/assets/d6c47d10-7e0e-4e37-b0dd-4867665d81c7)

The playback of the returns around month date
![image](https://github.com/user-attachments/assets/b6e086c2-713e-40e0-b921-06b98c6e43ca)

The sharpe of each playback window
![image](https://github.com/user-attachments/assets/9f27b369-37d6-47d2-95e1-b82b0f36cbbc)

Using an exponential moving average to infer the position yields almost the same sharpe
![image](https://github.com/user-attachments/assets/06c10b6e-2818-43f4-8036-06b2cc94ed3b)

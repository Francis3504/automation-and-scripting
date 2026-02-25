import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from merger import load_all_data

def pivot_table_data(merged_data:pd.DataFrame):
    pivot_table=pd.pivot_table(merged_data.dropna(),index="product",values="total",columns="regions",aggfunc="sum")
    pivot_table.to_csv("pivot_table.csv",index=True)
    pivot_chart=pivot_table.plot(kind="bar")
    
    pivot_chart.set_title("Annual Report.\nA Bar Chart  of  a Pivot Table.\nTotal sales by product across five regions in a year.")
    pivot_chart.set_xlabel("Product")
    pivot_chart.set_ylabel("Total revenue")

    figure1=pivot_chart.figure
    figure1.tight_layout()
    figure1.savefig("pivot_chart.png",)

def summary_sheet_data(merged_data:pd.DataFrame):
    summary=(merged_data.groupby(["regions","product"],dropna=True,as_index=True).agg(total_quantity=("quantity","sum"),
    total_revenue=("total","sum"),transactions=("invoice_id","count")).reset_index())   

    summary.to_csv("summary_sheet.csv",index=False)

    chart=summary.plot(x="product",y="total_quantity",kind="line",legend=True)

    chart.set_title("Annual Report.\nA Bar Chart  of  a Summary Sheet.\nTotal sales by product across five regions in a year.")
    chart.set_xlabel("Product")
    chart.set_ylabel("Total revenue")
    figure2=chart.figure
    figure2.tight_layout()
    figure2.savefig("summary_chart.png")

def main():
    
    folder=Path(r"C:\Users\tradelist\programming\automation and scripting\csv-merger\seed_data")
    merged=load_all_data(folder)
    merged.to_csv("merged_data.csv",index=False)
    pivot_table_data(merged)
    summary_sheet_data(merged)
    plt.show()
    

if __name__=="__main__":
    main()
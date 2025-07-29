import pandas as pd
import sqlite3
from Ingestion_db01 import ingest_db


def create_summary(conn):
  vendor_sales_summary = pd.read_sql_query(
        """ WITH FreightSummary AS (
    select
        VendorNumber,
        sum(Freight) as freight_cost
    FROM vendor_invoice 
    GROUP BY VendorNumber
),

   PurchaseSummary AS (
    SELECT
       p.VendorNumber,
       p.VendorName,
       p.Brand ,
       p.Description,
       p.PurchasePrice ,
       pp.Volume,
       pp.Price as ActualPrice,
       SUM(p.Quantity) as TotalPurchaseQuantity,
       SUM(p.Dollars) as TotalPurchaseDollars
    FROM purchases as p
    JOIN purchase_prices as pp
       ON p.Brand= pp.Brand
    WHERE p.PurchasePrice>0
    GROUP BY p.VendorNumber,p.VendorName,p.Brand ,p.Description,p.PurchasePrice ,pp.Volume,pp.Price
),

SalesSummary AS(
    SELECT
        VendorNo,
        Brand ,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars)  AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo,Brand 
)

SELECT
       ps.VendorNumber,
       ps.VendorName,
       ps.Brand ,
       ps.Description,
       ps.PurchasePrice ,
       ps.Volume,
       ps.ActualPrice,
       ps.TotalPurchaseQuantity,
       ps.TotalPurchaseDollars,
       ss.TotalSalesQuantity,
       ss.TotalSalesDollars,
       ss.TotalSalesPrice, 
       ss.TotalExciseTax,
       fs.freight_cost 
    FROM  PurchaseSummary ps
    LEFT JOIN  SalesSummary ss
       ON   ps.VendorNumber=ss.VendorNo
       AND ps.Brand= ss.Brand
    LEFT JOIN  FreightSummary fs
       ON  ps.VendorNumber=fs.VendorNumber                
    ORDER BY   ps.TotalPurchaseDollars DESC """,
        conn,)
  return vendor_sales_summary 




def clean_data(df):
    #cleaning data
    df["Volume"]= df["Volume"].astype('float64')
    df.fillna(0, inplace=True)
    df["VendorName"]=df["VendorName"].str.strip()
    
    #creating new columns 
    vendor_sales_summary["GrossProfit"] = vendor_sales_summary["TotalSalesDollars"] - vendor_sales_summary["TotalPurchaseDollars"]
    
    vendor_sales_summary["ProfitMargin"]=(vendor_sales_summary["GrossProfit"]/ vendor_sales_summary["TotalSalesDollars"])*100
    
    vendor_sales_summary["StockTurnover"]= vendor_sales_summary["TotalSalesQuantity"]/vendor_sales_summary["TotalPurchaseQuantity"]
    
    vendor_sales_summary["SalesToPurchaseRatio"]=vendor_sales_summary["TotalSalesDollars"]/vendor_sales_summary["TotalPurchaseDollars"]
    
    return df
    
    
if __name__=="__main__":
    #creating database connection
    
    conn =sqlite3.connect("inventoryy.db")
    summary_df=create_summary(conn)
    clean_df= clean_data(summary_df)
    ingest_db(clean_data,"vendor_sales_summary",conn)

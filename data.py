# Data Processing Packages
import pandas as pd
import numpy as np

# Helper Functions


def convert_datetime(df, col_name):
    df[col_name] = pd.to_datetime(df[col_name])


def set_datetime_index(df, col_name):
    df[col_name] = pd.to_datetime(df[col_name])
    df.set_index(col_name, inplace=True,  drop=True)
    df.set_index(pd.to_datetime(df.index), inplace=True)


class Data:

    def __init__(self):
        # List of unique listings
        hose_listings = pd.read_csv("cleaned data/HOSE_Stocks.csv")
        hnx_listings = pd.read_csv("cleaned data/HNX_Stocks.csv")

        hose_listings.drop(columns=["Unnamed: 0"], inplace=True)
        hnx_listings.drop(columns=["Unnamed: 0"], inplace=True)

        convert_datetime(hose_listings, "IPO_Date")
        convert_datetime(hnx_listings, "IPO_Date")

        hnx_listings.fillna(value={"Sector": "Unknown"}, inplace=True)
        hose_listings.fillna(value={"Sector": "Unknown"}, inplace=True)

        convert_datetime(hose_listings, "Delisting_Date")
        convert_datetime(hnx_listings, "Delisting_Date")
        hnx_listings.replace("HASTC", "HNX", inplace=True)
        # Daily Records of Exchange Indeces for HOSE and HNX
        hose = pd.read_csv("cleaned data/VNINDEX_2000_2020.csv")
        hnx = pd.read_csv("cleaned data/HNXINDEX_2005_2020.csv")

        set_datetime_index(hose, "Date")
        set_datetime_index(hnx, "Date")

        self.hose_listings = hose_listings
        self.hnx_listings = hnx_listings
        self.hnx = hnx
        self.hose = hose

    def get_all_records(self):
        # Daily Trading records of all Stocks from Cafef
        # HOSE Link: "https://raw.githubusercontent.com/plklinh/data501/main/HOSE_daily_trade.csv"
        # HOSE local link: "HOSE_daily_trade.csv"
        # HNX Link: "https://raw.githubusercontent.com/plklinh/data501/main/HNX_daily_trade.csv"
        # HNX Local Link: "HNX_daily_trade.csv"
        hose_all = pd.read_csv(
            "https://raw.githubusercontent.com/plklinh/data501/main/HOSE_daily_trade.csv")
        hnx_all = pd.read_csv(
            "https://raw.githubusercontent.com/plklinh/data501/main/HNX_daily_trade.csv")
        hnx_all.rename(columns={'Close_Price': "Close",
                                'Average_Price': "Avg", 'Reference_Price': "Adj_close", 'Open_Price': "Open", 'High_Price': "High",
                                'Low_price': "Low"}, inplace=True)
        hose_all.rename(columns={'Close_Price': "Close",
                                 'Average_Price': "Avg", 'Reference_Price': "Adj_close", 'Open_Price': "Open", 'High_Price': "High",
                                 'Low_price': "Low"}, inplace=True)
        convert_datetime(hose_all, "Date")
        convert_datetime(hnx_all, "Date")
        hose_all.drop(columns=["Unnamed: 0", "Unnamed: 0.1"], inplace=True)
        hnx_all.drop(columns=["Unnamed: 0",	"Unnamed: 0.1"], inplace=True)
        return hose_all,  hnx_all

    def get_trading_partners(self):
        trade_df = pd.read_csv("cleaned data/Trading_Partners_2000_2020.csv")
        set_datetime_index(trade_df, "Date")
        trade_df = trade_df.merge(self.hose[["Close"]], on="Date", how="outer")
        trade_df = trade_df.merge(
            self.hnx[["Close"]], on="Date", how="outer", suffixes=["_HOSE", "_HNX"])
        trade_df = trade_df.rename(
            columns={"Close_HOSE": "VNX-INDEX", "Close_HNX": "HNX-INDEX"})
        return trade_df

    def get_macro_df(self):
        macro_df = pd.read_csv("index_macro_merged.csv")
        set_datetime_index(macro_df, "Unnamed: 0")
        return macro_df

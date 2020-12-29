import pandas as pd,datetime
import pandas_datareader as pdr


def market_abuse_filtering(df, stock_name=None, date_ranges=None):
    """
    Pipeline of transformations for returning .
        - read stock_name data from yahoo and read traders file
        - remove invalid values
        - parse/normalize date column for both
        - join
        - filter out and return based on abuse conditionals

    :param

    df : traders file

    stock_name: custom stock names for the function(future scope)

    date_ranges: date range to get yahoo stocks(future scope)

    """
    # read yahoo details for stock_name

    pd.set_option('display.max_columns', None)

    try:
        amzn = pdr.get_data_yahoo("AMZN", start="2020-02-01",
                                  end="2020-03-31").reset_index()
    except:
        print("pdr get data yahoo error")

    # drop rows with null values for indispensable cols() for records

    traders = df.dropna(subset=["tradeDatetime", "stockName", "price",
                                "traderId", "tradeId", "volume"])

    # fixing pandas joining col "date"

    traders["trading_date"] = pd.to_datetime(traders["tradeDatetime"], format=
    "%Y-%m-%d %H:%M:%S").dt.date
    amzn["Date"] = pd.to_datetime(amzn["Date"], format="%Y-%m-%d").dt.date

    # joining based on date post filtering traders on stockname "Amazon" and
    # date_ranges (get stockname and date ranges from fn call)

    traders = traders[(traders["stockName"] == "Amazon")
            & (traders["trading_date"] >= datetime.date(2020, 2, 1))
            & (traders['trading_date'] <= datetime.date(2020, 3, 31))]

    df = pd.merge(traders,
                  amzn,left_on='trading_date', right_on='Date',
                  how='left')

    # checking filtering conditions for suspicious buyers
    df2 = df[(df["High"] < df["price"])  # check if buying price>trading max
             | (df["Low"] > df["price"])  # check if buying price<trading min
             | (df["Date"].isnull())]  # check if buying day is non trading day

    print(df2[["Date","High","Low","price","tradeId","traderId",
               "trading_date"]].head())

    return df2


def market_abuse_aggregation(df, agg_parameter_list=None):
    """
        Pipeline of transformations for returning .
            - assign an extra col sus
            - select cols and group by traderid and calculate counts sort them
            - select cols and group by countryid and calculate counts sort them

        :param

        df: downstream filtered suspect dataframe
        agg_parameter_list : used for writing custom agg groups(future scope)
    """

    df2 = df.assign(sus="Yes")

    # selecting cols and grouping sus on traderids and sorting counts
    suspect_traderid_count = df2[["traderId", "sus"]].groupby(
        by=["traderId"]).count().sort_values(by="sus",
                                             ascending=False)

    # selecting cols checking traderid and countrycode groups
    # df2[["traderId", "sus", "countryCode"]].groupby(
    #     by=["traderId", "countryCode"]).count().sort_values(by="sus",
    #                                                         ascending=False)

    print(suspect_traderid_count.head())

    # selecting cols and grouping sus on countrycode and sorting counts

    suspect_cntryid_count = df2[["countryCode", "sus"]].groupby(
        by=["countryCode"]).count().sort_values(by="sus", ascending=False)
    print(suspect_cntryid_count.head())

    # can return both dfs based on requirement


if __name__ == "__main__":

    try:
        df_m = pd.read_csv("files/traders_data.csv")
    except FileNotFoundError:
        print("File path incorrect")
    filtered_sus = market_abuse_filtering(df_m)
    market_abuse_aggregation(filtered_sus)

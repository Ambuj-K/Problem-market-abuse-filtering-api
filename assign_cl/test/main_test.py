import unittest
from assign_cl.main import *
from pandas import DataFrame


def test_main():
    pdf_1 = DataFrame(data={
            'countryCode': ["JP", "NZ","TV","BT"],
            'firstName': ['Christine', 'Brittany',"Vanessa","Allison"],
            'lastName': ['Baldwin', 'Herring',"Green","Davis"],
            'traderId': ['vDAqsRnDaMrcomXsosXG', 'pjjFIyeNTWRUWCuKoQSU',
                         "MUVGtYHdeMdauAxzEJvt","vDAqwRnDeMewcosssosXG"],
            'stockSymbol': ['AMZN', 'AMZN','AMZN', 'AMZN'],
            'stockName': ['Amazon', 'Amazon', 'Amazon', 'Amazon'],
            'tradeId': ['r8-6474936t', 'U9-4261680I', 'U4-1266883G', "Z0-494338r"],
            'price': [1766.000000, 2351.14450,1701.76000, 1707.86000],
            'volume': [295.0, 154.0, 58.0,29.0],
            'tradeDatetime': ['2020-02-19 18:13:34', '2020-02-10 17:42:11',
                              '2020-04-03 07:03:32',"2020-02-29 08:04:30"],
        })

    intr_df = market_abuse_filtering(pdf_1)
    market_abuse_aggregation(intr_df)

    # assert()


if __name__ == "__main__":
    test_main()

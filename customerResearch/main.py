import os

import pandas as pd
import numpy


def join_sets_on_subscription_id(how_to_join):
    # TODO: paste absolute paths (don't forget to change / to \\ if copying in windows)
    msft_owned_subs = pd.read_csv(
        "C:\\Users\\urandjelovic\\OneDrive - Microsoft\\Desktop\\application-name-analyisis\data\\all_MICROSOFT_subscriptions_expanded_dpdw.csv")
    kusto_exported_subs_all_regions = pd.read_csv(
        "C:\\Users\\urandjelovic\\OneDrive - Microsoft\\Desktop\\application-name-analyisis\\data\\applicationNames-sqlazurewus1-487526_kusto.csv")

    msft_owned_subs['SubscriptionGUID'] = msft_owned_subs['SubscriptionGUID'].str.upper()
    kusto_exported_subs_all_regions['SubscriptionId'] = kusto_exported_subs_all_regions['SubscriptionId'].str.upper()

    # msft_owned_subs.set_option("display.max_columns", None)
    # kusto_exported_subs_all_regions.set_option("display.max_columns", None)

    print(msft_owned_subs.head())
    print(kusto_exported_subs_all_regions.head())

    subs_from_kusto_enriched_with_dpdw_data = pd.merge(
        kusto_exported_subs_all_regions, msft_owned_subs,
        how=how_to_join, left_on="SubscriptionId", right_on="SubscriptionGUID")

    print(subs_from_kusto_enriched_with_dpdw_data)

    subs_from_kusto_enriched_with_dpdw_data.to_csv('./csv_dumps/microsoft_ids_in_joined_set.csv', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    join_sets_on_subscription_id(how_to_join='outer')


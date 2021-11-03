import os

import pandas as pd
import numpy


def join_sets_on_subscription_id(how_to_join):
    # TODO: paste absolute paths (don't forget to change / to \\ if copying in Windows environment)
    path_to_folder = "C:\\Users\\urandjelovic\\OneDrive - Microsoft\\Desktop\\application-name-analyisis\data\\"

    msft_owned_subs_file = "all_MICROSOFT_subscriptions_expanded_dpdw.csv"
    kusto_exported_subs_all_regions = "applicationNames-sqlazurewus1-487526_kusto.csv"

    msft_owned_subs = pd.read_csv(path_to_folder + msft_owned_subs_file)
    kusto_exported_subs_all_regions = pd.read_csv(path_to_folder + kusto_exported_subs_all_regions)

    msft_owned_subs['SubscriptionGUID'] = msft_owned_subs['SubscriptionGUID'].str.upper()
    kusto_exported_subs_all_regions['SubscriptionId'] = kusto_exported_subs_all_regions['SubscriptionId'].str.upper()

    print(msft_owned_subs.head())
    print(kusto_exported_subs_all_regions.head())

    subs_from_kusto_enriched_with_dpdw_data = pd.merge(
        kusto_exported_subs_all_regions, msft_owned_subs,
        how=how_to_join, left_on="SubscriptionId", right_on="SubscriptionGUID")

    print(subs_from_kusto_enriched_with_dpdw_data)

    return subs_from_kusto_enriched_with_dpdw_data


def get_df_with_dropped_rows_matching_a_condition(dataframe, column_name, value_to_match):
    print("Dropping rows where" + column_name + "contains" + value_to_match)
    dataframe[column_name] = dataframe[column_name].str.lower()
    value_to_match = value_to_match.lower()

    # df_without_dropped_values = dataframe[dataframe[column_name].str.contains("|".join([value_to_match.lower()]))]
    df_without_dropped_values = dataframe[~dataframe[column_name].str.contains('|'.join([value_to_match]), na=False)]

    print('___________________________')
    print('df_without_dropped_values')
    print(df_without_dropped_values)
    print('___________________________')

    return df_without_dropped_values


def save_dataframe_to_disk(pandas_frame, filename):
    folder_to_write = './csv_dumps/'
    print(filename + ' writing to: ' + folder_to_write)
    # TODO: if the folder csv_dumps does not get created by pandas, create it manually
    pandas_frame.to_csv(folder_to_write + filename + '.csv', index=False)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    junk_terms = [
        "Microsoft SQL Server",
        "AzureDataMovement",
        "Telegraf",
        "Internet Information Services",
        "Gi FileLoad",
        "Datadog Agent",
        "Go - mssqldb",
        "Driver",
        "Sql Agent",
        "SqlAgent",
        "Sql MP",
        "Display Catalog",
        "Data Provider",
        "pccsql",
        "EntityFramework",
        "SalesExec",
        "ArcGis",
        "SqlQuery",
        "SolarWinds"
    ]

    joined_sets = join_sets_on_subscription_id(how_to_join='left')

    dropped_internal_customers = get_df_with_dropped_rows_matching_a_condition(joined_sets,
                                                                               column_name="OrganizationName",
                                                                               value_to_match="MICROSOFT")
    dropped_junk_terms = dropped_internal_customers

    for junk_term in junk_terms:
        dropped_junk_terms = get_df_with_dropped_rows_matching_a_condition(dropped_junk_terms,
                                                                           column_name="ApplicationName",
                                                                           value_to_match=junk_term)

    save_dataframe_to_disk(dropped_junk_terms, 'onlyValuableExternalCustomers.csv')

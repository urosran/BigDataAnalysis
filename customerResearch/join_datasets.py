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


def get_df_with_dropped_rows_matching_a_condition(dataframe, column_name, value_to_drop):
    print("Dropping rows where" + column_name + "contains" + value_to_drop)
    dataframe[column_name] = dataframe[column_name].str.lower()

    value_to_drop = value_to_drop.lower()

    # df_without_dropped_values = dataframe[dataframe[column_name].str.contains("|".join([value_to_match.lower()]))]
    df_without_dropped_values = dataframe[~dataframe[column_name].str.contains('|'.join([value_to_drop]), na=False)]

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
        "SolarWinds",
        "DacFx",
        "DataAccess",
        "DatabaseMail",
        "dbtools",
        'dev-webhook',
        'dw-etl',
        'databasemail',
        'sharepoint',
        'azdata',
        'sqleditor',
        'python',
        'node',
        'php',
        'apache',
        'jobservice',
        'loanspq',
        'operating system',
        'notification',
        'mashup',
        'microsoft office',
        '/',
        '-',
        'msdb',
        'master',
        'tempdb',
        'system.activities',
        'defaultpool',
        'localbuildtest',
        'debugging',
        'windowsapplication1',
        'tedious',
        'sqlcmd',
        'websphere application server',
        'vadmin',
        'alteryx',
        'lenosmodel',
        'stdioprocessor',
        'one identity',
        'report server',
        'replication monitor',
        'discovery hub',
        'jtds',
        'dbupdate',
        'meded',
        'notifyhandler',
        'tabprotosrv',
        'visual studio',
        'axdixf',
        'gtkschedulereports',
        'unit testing',
        'ignitemonitor',
        'report server',
        'safeway .net framework',
        'sql management',
        'sendcryptokeytoserver',
        'pearlabyss_blackdesert_real',
        'apexsql source control',
        'power automate desktop',
        'supportpoint',
        'laserfiche',
        'mediapulse',
        'snapshot',
        'sqlsession',
        'agent',
        'sql server performance investigator',
        'extract',
        'replicat',
        'persec',
        'data',
        'sync',
        'foglight',
        'linqpad',
        'compliancemapenterprise',
        'leadcontactassignmentaudit',

    ]

    useful_but_too_many = [
        "campusnexus student",
        "emerson",
        'hankisoft.com',
        'red gate',
        'redgate',
        'microsoft dynamics 365',
        'archertech',
        'dbforge',
        'tibco',
        'informatica',
        'tableau',
        'pearlabyss_blackdesert_real',

    ]

    joined_sets = join_sets_on_subscription_id(how_to_join='left')

    # remove empty rows
    joined_sets.dropna(subset=['ApplicationName'], inplace=True)

    dropped_internal_customers = get_df_with_dropped_rows_matching_a_condition(joined_sets,
                                                                               column_name="OrganizationName",
                                                                               value_to_drop="MICROSOFT")
    dataset_without_specified_terms = dropped_internal_customers

    for junk_term in junk_terms:
        dataset_without_specified_terms = get_df_with_dropped_rows_matching_a_condition(dataset_without_specified_terms,
                                                                                        column_name="ApplicationName",
                                                                                        value_to_drop=junk_term)

    for useful_term in useful_but_too_many:
        dataset_without_specified_terms = get_df_with_dropped_rows_matching_a_condition(dataset_without_specified_terms,
                                                                                        column_name="ApplicationName",
                                                                                        value_to_drop=useful_term)

    dataset_without_specified_terms.sort_values(by="SubscriptionId", inplace=True)

    save_dataframe_to_disk(dataset_without_specified_terms, 'onlyValuableExternalCustomers.csv')

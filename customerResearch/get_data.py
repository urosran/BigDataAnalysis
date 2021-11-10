# import modules

# Client, Connection
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

# Catching KustoServiceError is useful when making retry logic
from azure.kusto.data.exceptions import KustoServiceError

# Helper function that transforms result into pandas DataFrame
from azure.kusto.data.helpers import dataframe_from_result_table

import pandas as pd
# Kusto connection string should by of the format: https://<clusterName>.kusto.windows.net
cluster_name = 'https://sqlazurewus1.kusto.windows.net'
kustoStringBuilder = KustoConnectionStringBuilder.with_aad_device_authentication(cluster_name)
kusto_client = KustoClient(kustoStringBuilder)

query_text = \
    "MonLogin"
"    | where AppTypeName == \"Worker.CL\" or AppTypeName == \"Gateway.PDC\""
"    | where event == \"process_login_finish\""
"    | extend Date = startofday(PreciseTimeStamp)"
"    | extend SubscriptionId = iff(AppTypeName == \"Gateway.PDC\", subscription_id, SubscriptionId)"
"    | extend LogicalServerName = tolower(iff(AppTypeName == \"Gateway.PDC\", logical_server_name, LogicalServerName))"
"    | where SubscriptionId != \"00000000-0000-0000-0000-000000000000\""
"    | extend application_name = iff("
"        application_name startswith \"SQLAgent - TSQL JobStep\", \"SQL Agent Job\","
"        iff(application_name startswith \"Microsoft SQL Server Management Studio\", \"Microsoft SQL Server Management Studio\", application_name)"
"        )"
"    | where application_name !contains \"data provider\""
"    | summarize"
"        SuccessfulLoginCount = countif(is_success == 1)"
"        //FailedLoginCount = countif(is_success == 0),"
"        //DatabaseCount = dcount(database_name)"
"        by"
"        LogicalServerName,"
"        SubscriptionId,"
"        ApplicationName = application_name,"
"        database_name,"
"        DriverName = driver_name,"
"        Date;"

response = kusto_client.execute("sqlazure1", query_text)

df = dataframe_from_result_table(response.primary_results[0])

from azure.common.credentials import ServicePrincipalCredentials
# from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
import settings

import time

def InitiateDatafactoryClient():

    #azure credentials from local-sp.json for nathans local laptop
    clientId = settings.clientId
    clientSecret = settings.clientSecret
    subscriptionId = settings.subscriptionId
    tenantId = settings.tenantId

    #needs to be replaced with azure.identity
    #https://stackoverflow.com/questions/64063850/azure-python-sdk-serviceprincipalcredentials-object-has-no-attribute-get-tok
    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ServicePrincipalCredentials(client_id= clientId, secret= clientSecret, tenant= tenantId)

    # resource_client = ResourceManagementClient(credentials, subscriptionId)
    adf_client = DataFactoryManagementClient(credentials, subscriptionId)

    return adf_client

def RunPipeline(adf_client,pipelinename,delaystart,delayrunstatuscheck):

    #delays start in case blob is not recognized
    time.sleep(delaystart)

    rg_name = settings.rg_name

    datafactory_name = settings.datafactory_name

    run_response = adf_client.pipelines.create_run(rg_name,datafactory_name,pipelinename)

    #accounts for time to spin up cluster
    time.sleep(delayrunstatuscheck)

    pipeline_run = adf_client.pipeline_runs.get(rg_name, datafactory_name, run_response.run_id)

    return print("\n\tPipeline run status: {}".format(pipeline_run.status))
# This is Onfido's extract to CSV
# Schedule: Every month on 27th and again at the last day of that month for the remainder of the days in that month
# Extracts billing info (Subsidiary Id, SF Account Id, Netsuite Product Code, Quantity, Price and Date)
# Output: csv

import logging
import sys
import json
import boto3
import os
from io import StringIO
from datetime import datetime, timedelta
import m3terSDK as m3ter
import pandas as pd
from sqlalchemy import create_engine
import re

# Setup Logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# End

# The utility runs on AWS lambda (use main() for manual runs)
# AWS config is in serverless.yml - for more details, reach out to Dávid K or Jamie C
# s3_resource = boto3.resource('s3')
# bucket = os.getenv('S3_BUCKET')


# Functions for logs - local CSV or S3_bucket on AWS
def df_to_s3(df, filename):
    # if bucket:
    #   csv_buffer = StringIO()
    #  df.to_csv(csv_buffer)
    # s3_resource.Object(bucket, filename).put(Body=csv_buffer.getvalue())
    # else:
    # logger.debug('S3_BUCKET not set. Running locally.')
    df.to_csv(f"logs/{filename}", mode="w", index=False)




def main():
    m3ter.printme('Starting execution ', time=True, color='red', dots=True)

    # read from onfido aurora database to find netsuite product ids and netsuite bundle id
    connection = m3ter.openSqlAlchemy()
    currentSchema = os.environ['currentSchemaName']
    productData_df = pd.read_sql_table(table_name='input_activeproducts', con=connection, schema=currentSchema)
    bundleData_df = pd.read_sql_table(table_name='bill_netsuite_xref', con=connection, schema=currentSchema)

    bills = m3ter.Bill().load()

    bills_df = pd.json_normalize(bills, record_path='lineItems',
                                 meta=['id', 'version', 'accountId', 'accountCode',
                                       'startDate', 'endDate', 'startDateTimeUTC',
                                       'endDateTimeUTC', 'billDate', 'dueDate',
                                       'billingFrequency', 'billFrequencyInterval',
                                       'timezone', 'currency', 'locked', 'createdDate',
                                       'status', 'billJobId', 'lastCalculatedDate'],
                                 errors='ignore', record_prefix='lineItems-')

    # BilDate == yesterday
    yday = str((datetime.today() - timedelta(days=1)))
    yday = yday.split(" ")[0]
    bills_df = bills_df.loc[bills_df.billDate == yday]

    pricingBand_df = pd.json_normalize(bills, ['lineItems', 'usagePerPricingBand'],
                                       record_prefix='lineItems-usagePerPricingBand-')
    bills_df['lineItems-usagePerPricingBand-unitPrice'] = pricingBand_df['lineItems-usagePerPricingBand-unitPrice']

    bills_df_columns = bills_df[
        ['id', 'accountId', 'accountCode', 'lineItems-productId', 'lineItems-quantity', 'lineItems-productName',
         'lastCalculatedDate', 'lineItems-usagePerPricingBand', 'lineItems-description', 'lineItems-meterId',
         'lineItems-usagePerPricingBand-unitPrice', 'lineItems-planId']]

    # Reformat lastCalculatedDate and UnitPrice
    bills_df_columns['lastCalculatedDate'] = pd.to_datetime(bills_df_columns['lastCalculatedDate']).dt.strftime('%d/%m/20%y')
    bills_df_columns = bills_df_columns.round(2)

    account = m3ter.Account().load()
    account_df = pd.json_normalize(account)
    account_df_columns = account_df[['id', 'customFields.subsidiaryId']]

    meter = m3ter.Meter().load()
    meter_df = pd.json_normalize(meter)
    meter_df.columns = meter_df.columns.str.replace('id', 'meterId')
    meter_df.columns = meter_df.columns.str.replace('code', 'meter-code')

    plan = m3ter.Plan().load()
    plan_df = pd.json_normalize(plan)
    plan_df.columns = plan_df.columns.str.replace('id', 'planId')

    # merge all tables
    dataExfiltration = bills_df_columns.merge(account_df_columns, how='left', left_on='accountId', right_on='id')
    dataExfiltration = dataExfiltration.merge(plan_df, how='left', left_on='lineItems-planId', right_on='planId')
    dataExfiltration = dataExfiltration.merge(meter_df, how='left', left_on='lineItems-meterId', right_on='meterId')
    dataExfiltration = dataExfiltration.merge(productData_df, how='left', left_on='meter-code', right_on='Meter_Code__c')
    dataExfiltration = dataExfiltration.merge(bundleData_df, how='left', left_on='code', right_on='opportunityId')

    # NC Addition ------------------------
    dataExfiltration['Netsuite_Product_Id__c'] = dataExfiltration['Netsuite_Product_Id__c'].fillna("0")
    dataExfiltration['Netsuite_Product_Id__c'] = dataExfiltration['Netsuite_Product_Id__c'].astype(int)

    df_to_s3(dataExfiltration, 'lineItems.csv')

    dataExfiltration = dataExfiltration[
        ['customFields.subsidiaryId', 'accountCode', 'Netsuite_Product_Id__c', 'netsuiteId', 'lineItems-quantity',
         'lineItems-usagePerPricingBand-unitPrice', 'lastCalculatedDate']]

    # data cleanup
    dataExfiltration.columns = dataExfiltration.columns.str.replace('accountCode', 'SF Account ID')
    dataExfiltration.columns = dataExfiltration.columns.str.replace('Netsuite_Product_Id__c', 'Netsuite Product Code')
    dataExfiltration.columns = dataExfiltration.columns.str.replace('lineItems-quantity', 'Quantity')
    dataExfiltration.columns = dataExfiltration.columns.str.replace('lineItems-usagePerPricingBand-unitPrice', 'Price')
    dataExfiltration.columns = dataExfiltration.columns.str.replace('lastCalculatedDate', 'Date')
    dataExfiltration.columns = dataExfiltration.columns.str.replace('customFields.subsidiaryId', 'Subsidiary ID')

    dataExfiltration['Subsidiary ID'] = dataExfiltration['Subsidiary ID'].astype(int)

    # dropping null prices
    dataExfiltration.drop(dataExfiltration.loc[dataExfiltration['Price'] == 0].index, inplace=True)
    # drop rows with 0 in the netsuite product code - aka. bundles
    dataExfiltration = dataExfiltration[dataExfiltration['Netsuite Product Code'] != 0]

    df_to_s3(dataExfiltration, 'dataExfiltration.csv')

    m3ter.printme('Execution complete ', time=True, color='red', dots=True)


if __name__ == '__main__':
    main()
import os
import requests
import datetime
import json
import pandas as pd
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2

"""
This is a fork of the m3terSDK from the Customer Onboarding Framework project
It has been modified as per the project requirements.
You can find the original SDK here: https://github.com/m3ter-labs/customer-onboarding-framework
"""

logger = logging.getLogger()
logfile = None

# load_dotenv("config/config.env")
load_dotenv("config/config_prod.env")

# Set up key variables that are used in all classes and methods
ENVIRONMENT = os.getenv('ENVIRONMENT')
ORGANIZATION = os.getenv('ORGANIZATION')
LOGGING = True
if ENVIRONMENT == 'prod':
    root_api_url = "https://api.m3ter.com/organizations/" + ORGANIZATION
    ingest_api_url = "https://ingest.m3ter.com/organizations/" + ORGANIZATION
else:
    root_api_url = "https://api." + ENVIRONMENT + ".m3ter.com/organizations/" + ORGANIZATION
    ingest_api_url = "https://ingest." + ENVIRONMENT + ".m3ter.com/organizations/" + ORGANIZATION


def getToken(username, password):
    headers = {
        'Content-Type': 'application/json'
    }
    if ENVIRONMENT == 'prod':
        url = "https://api.m3ter.com/oauth/token"
    else:
        url = "https://api." + ENVIRONMENT + ".m3ter.com/oauth/token"

    data_raw = '{"grant_type": "client_credentials"}'
    response = requests.post(url=url, headers=headers, auth=(username, password), data=data_raw)
    return response.json().get('access_token')


def executeAPI(action, token, url, payload):
    headers = {
        'Authorization': 'Bearer ' + token,
        'Content-Type': 'application/json'
    }
    response = requests.request(action, url, headers=headers, data=payload)
    print(response.text)
    return response


def logWrite(logfile, entity, action, payload, status, response, url):
    logger.debug(f'{action} {entity} .....................')
    if payload: logfile.write('\n' + payload)
    if status:
        if status != 200:
            logger.debug('\nStatus: ' + str(status))
    if response: logger.debug('\n' + response)
    if url:
        logger.debug('\nURL: ' + url)


def printme(input='', color=None, dots=False, time=False):
    DEBUG = True
    if time:
        ts = datetime.datetime.now()
        input = str(ts) + ' - ' + input
    # Create dots of correct length
    if dots:
        dots = ' '
        for i in range(0, 80 - len(input)):
            dots = dots + '.'
        input = input + dots
    # Set color codes
    csi = '\x1B['
    red = csi + '31;1m'
    green = csi + '32;1m'
    yellow = csi + '33;1m'
    blue = csi + '34;1m'
    purple = csi + '35;1m'
    cyan = csi + '36;1m'
    end = csi + '0m'
    if color == 'red':
        input = red + input + end
    elif color == 'yellow':
        input = yellow + input + end
    elif color == 'green':
        input = green + input + end
    elif color == 'blue':
        input = blue + input + end
    elif color == 'purple':
        input = purple + input + end
    elif color == 'cyan':
        input = cyan + input + end
    if DEBUG:
        logger.debug(input)


TOKEN = getToken(username=os.getenv('apiKey'), password=os.getenv('apiSecret'))
if LOGGING:
    logger.debug('\nEnvironment: ' + ENVIRONMENT)
    logger.debug('\nOrganization: ' + ORGANIZATION)

def openSqlAlchemy():
    dbname = os.getenv('dbname')
    options = os.getenv('dboptions')
    user = os.getenv('dbuser')
    password = os.getenv('dbpassword')
    host = os.getenv('dbhost')
    port = os.getenv('dbport')
    connection = 'postgresql+psycopg2' + '://' + os.getenv('dbuser') + ':' + os.getenv('dbpassword') + '@' + os.getenv('dbhost') + ':' + os.getenv('dbport') + '/' + os.getenv('dbname')
    connection_args = dict()
    connection_args['options'] = os.getenv('dboptions')
    try:
        engine = create_engine(connection, connect_args=connection_args, pool_size=20, max_overflow=40)
        return engine
    except:
        printme('SQLAlchemy - Unable to open connection for: ' + str(connection) + ' ' + str(connection_args))
        return None

def openPG():
    print('Executing function: openPG()')
    connection = psycopg2.connect(dbname=os.getenv('dbname'),
                                  options=os.getenv('dboptions'),
                                  user=os.getenv('dbuser'),
                                  password=os.getenv('dbpassword'),
                                  host=os.getenv('dbhost'),
                                  port=os.getenv('dbport'))

    print('Connection status: ' + str(connection.closed))
    return connection


class M3terAPI:
    def create(self):
        url = root_api_url + self.class_url
        payload = json.dumps(self.__dict__)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Creating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def list(self, nextToken=None):
        if nextToken:
            url = root_api_url + self.class_url + '?nextToken=' + nextToken
        else:
            url = root_api_url + self.class_url
        payload = None
        response = executeAPI(action="GET", token=TOKEN, url=url, payload="")
        if LOGGING: logWrite(logfile=logfile, entity=self.class_url[1:-1], action='Listing', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def get(self):
        url = root_api_url + self.class_url + "/" + self.id
        payload = None
        response = executeAPI(action="GET", token=TOKEN, url=url, payload="")
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Getting', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def delete(self):
        url = root_api_url + self.class_url + "/" + self.id
        payload = None
        try:
            response = executeAPI(action="DELETE", token=TOKEN, url=url, payload="")
        except:
            try:
                response = executeAPI(action="DELETE", token=TOKEN, url=url, payload="")
            except:
                response = executeAPI(action="DELETE", token=TOKEN, url=url, payload="")

        if LOGGING: logWrite(logfile=logfile, entity=self.class_url[1:-1], action='Deleting', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def update(self):
        url = root_api_url + self.class_url + "/" + self.id
        payload = None
        response = executeAPI(action="PUT", token=TOKEN, url=url, payload="")
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Updating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)


    def load(self, silent=False):
        next_token = None
        paging = True
        objects = []

        while paging:
            results = self.list(nextToken=next_token)
            if 'data' in results:
                for object in results['data']:
                    objects.append(object)
            if 'nextToken' in results:
                next_token = results['nextToken']
                paging = True
            else:
                paging = False

        if not silent: printme('#' + self.__class__.__name__ + '(s): ' + str(len(objects)), color='yellow', dots=True)
        return objects

    def codeGet(self, code):
        currentId = None
        objects = self.load(silent=True)
        for object in objects:
            if object['code'] == code:
                currentId = object['id']

        return currentId

    def nameGet(self, name):
        currentId = None
        objects = self.load(silent=True)
        for object in objects:
            if object['name'] == name:
                currentId = object['id']

        return currentId


class Product(M3terAPI):
    class_url = "/products"

    def __init__(self, name="", code="", id=""):
        self.name = name
        self.code = code
        self.id = id


class dataField:
    def __init__(self, category="", code="", unit="", name=""):
        self.category = category
        self.code = code
        self.unit = unit
        self.name = name


class derivedField:
    def __init__(self, category="", code="", unit=None, name="", calculation=""):
        self.category = category
        self.code = code
        if unit:
            self.unit = unit
        self.name = name
        self.calculation = calculation

    def todict(self):
        return self.__dict__


class Meter(M3terAPI):
    class_url = "/meters"

    def __init__(self, productId="", name="", code="", id=""):
        if productId:
            self.productId = productId
        self.name = name
        self.code = code
        self.id = id
        self.dataFields = []
        self.derivedFields = []

    def create(self, dataFields, derivedFields):
        self.dataFields = dataFields
        if derivedFields is None:
            self.derivedFields = []
        else:
            self.derivedFields = derivedFields
        url = root_api_url + self.class_url
        payload = json.dumps(self.__dict__)
        # print(payload)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Creating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)


class Aggregation(M3terAPI):
    class_url = "/aggregations"

    def __init__(self, meterId="", name="", code="", aggregation="", defaultValue=None, rounding="",
                 quantityPerUnit=1.0, unit="", targetField="", segmentedFields=[], segments=[], id=""):
        self.meterId = meterId
        self.name = name
        self.code = code
        self.aggregation = aggregation
        self.defaultValue = defaultValue
        self.rounding = rounding
        self.quantityPerUnit = quantityPerUnit
        self.unit = unit
        self.targetField = targetField
        self.segmentedFields = segmentedFields
        self.segments = segments
        self.id = id

    def update(self, version=1, segmentedFields=[], segments=[]):
        self.segmentedFields = segmentedFields
        self.segments = segments
        self.version = version
        url = root_api_url + self.class_url + "/" + self.id
        payload = json.dumps(self.__dict__)
        # print(payload)
        response = executeAPI(action="PUT", token=TOKEN, url=url, payload=payload)
        print(response.text)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Updating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def todict(self):
        return self.__dict__


class CompoundAggregation(M3terAPI):
    class_url = "/compoundaggregations"

    def __init__(self, productId="", code="", name="", rounding="", quantityPerUnit=1.0, unit="", calculation="",
                 id=""):
        self.productId = productId
        self.code = code
        self.name = name
        self.rounding = rounding
        self.quantityPerUnit = quantityPerUnit
        self.unit = unit
        self.calculation = calculation
        self.id = id


class PlanTemplate(M3terAPI):
    class_url = "/plantemplates"

    def __init__(self, productId="", name="", currency="USD", standingCharge=0, standingChargeInterval=1,
                 standingChargeOffset=0, billFrequency="MONTHLY", billFrequencyInterval=1, ordinal=0, id=""):
        self.productId = productId
        self.name = name
        self.currency = currency
        self.standingCharge = standingCharge
        self.standingChargeInterval = standingChargeInterval
        self.standingChargeOffset = standingChargeOffset
        self.billFrequency = billFrequency
        self.billFrequencyInterval = billFrequencyInterval
        self.ordinal = ordinal
        self.id = id


class PlanGroup(M3terAPI):
    class_url = "/plangroups"

    def __init__(self, name="", code="", accountId=None, standingCharge=0, minimumSpend=0, currency='USD', id=""):
        self.name = name
        self.code = code
        if accountId:
            self.accountId = accountId
        self.standingCharge = standingCharge
        self.minimumSpend = minimumSpend
        self.currency = currency
        self.id = id


class PlanGroupLink(M3terAPI):
    class_url = "/plangrouplinks"

    def __init__(self, planId="", planGroupId="", id=""):
        self.planId = planId
        self.planGroupId = planGroupId
        self.id = id


class Plan(M3terAPI):
    class_url = "/plans"

    def __init__(self, planTemplateId="", name="", code="", accountId=None, standingCharge=0, ordinal=0, bespoke=False,
                 minimumSpend=0, id=""):
        self.planTemplateId = planTemplateId
        self.name = name
        self.code = code
        if accountId:
            self.accountId = accountId
        self.standingCharge = standingCharge
        self.ordinal = ordinal
        self.bespoke = bespoke
        self.minimumSpend = minimumSpend
        self.id = id


class Contract(M3terAPI):
    class_url = "/contracts"

    def __init__(self, name="", code="", accountId=None, startDate="2022-01-01T00:00:00Z",
                 endDate="2022-01-01T00:00:00Z", id=""):
        self.name = name
        self.code = code
        if accountId:
            self.accountId = accountId
        self.startDate = startDate
        self.endDate = endDate
        self.id = id


class CreditType(M3terAPI):
    class_url = "/credittypes"

    def __init__(self, description="", code="", daysToExpiry=365, consumptionOrder="NEWEST_FIRST", id=""):
        self.description = description
        self.code = code
        self.daysToExpiry = daysToExpiry
        self.consumptionOrder = consumptionOrder
        self.id = id


class Credit(M3terAPI):
    class_url = "/credits"

    def __init__(self, accountId="", creditTypeId="", currentBalance=0, expiryTime="2022-01-01T00:00:00Z", id=""):
        self.accountId = accountId
        self.creditTypeId = creditTypeId
        self.currentBalance = currentBalance
        self.expiryTime = expiryTime
        self.id = id


class pricingBand:
    def __init__(self, lowerLimit=0.0, fixedPrice=0.0, unitPrice=0.0, creditTypeId=""):
        self.lowerLimit = lowerLimit
        self.fixedPrice = fixedPrice
        self.unitPrice = unitPrice
        self.creditTypeId = creditTypeId

    def todict(self):
        return self.__dict__


class Pricing(M3terAPI):
    class_url = "/pricings"

    def __init__(self, template=False, planId="", aggregationId=None, compoundAggregationId=None,
                 startDate="2022-01-01T00:00:00Z", endDate=None, cumulative=True, type=None, segment=None,
                 minimumSpend=0, description="", tiersSpanPlan=False, id=""):
        if aggregationId:
            self.aggregationId = aggregationId
        if compoundAggregationId:
            self.compoundAggregationId = compoundAggregationId
        self.startDate = startDate
        if endDate:
            self.endDate = endDate
        self.cumulative = cumulative
        self.description = description
        self.minimumSpend = minimumSpend
        if type:
            self.type = type
        if segment:
            self.segment = segment
        if template:
            self.planTemplateId = planId
        else:
            self.planId = planId
        if tiersSpanPlan:
            self.tiersSpanPlan = tiersSpanPlan
        self.id = id
        self.pricingBands = None

    def create(self, pricingBands):
        self.pricingBands = pricingBands
        url = root_api_url + self.class_url
        payload = json.dumps(self.__dict__)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Creating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)


class Address:
    def __init__(self, addressLine1="", addressLine2="", addressLine3="", addressLine4="", locality="", region="",
                 postCode="", country=""):
        self.addressLine1 = addressLine1
        self.addressLine2 = addressLine2
        self.addressLine3 = addressLine3
        self.addressLine4 = addressLine4
        self.locality = locality
        self.region = region
        self.postCode = postCode
        self.country = country

    def todict(self):
        return self.__dict__


class Account(M3terAPI):
    class_url = "/accounts"

    def __init__(self, name="", code="", emailAddress="", parentAccountId=None, address=None, customFields=None, id=""):
        self.name = name
        self.code = code
        self.emailAddress = emailAddress
        self.address = {}
        if parentAccountId:
            self.parentAccountId = parentAccountId
        if address:
            self.address = address
        if customFields:
            self.customFields = customFields
        self.id = id

    def create(self, address=None, customFields=None):
        if address:
            self.address = address
        if customFields:
            self.customFields = customFields
        url = root_api_url + self.class_url
        payload = json.dumps(self.__dict__)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Creating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def update(self, version=1, parentAccountId=None):
        if parentAccountId:
            self.parentAccountId = parentAccountId
        self.version = version
        url = root_api_url + self.class_url + "/" + self.id
        payload = json.dumps(self.__dict__)
        response = executeAPI(action="PUT", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Updating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)


class AccountPlan(M3terAPI):
    class_url = "/accountplans"

    def __init__(self, accountId="", planId="", planGroupId="", contractId=None, startDate="2022-01-01T00:00:00Z",
                 endDate="2029-01-01T00:00:00Z", id=""):
        self.accountId = accountId
        if planId:
            self.planId = planId
        if planGroupId:
            self.planGroupId = planGroupId
        if contractId:
            self.contractId = contractId
        self.startDate = startDate
        self.endDate = endDate
        self.id = id


class Commitment(M3terAPI):
    class_url = "/commitments"

    def __init__(self, accountId="", billingPlanId="", productIds=None, contractId=None,
                 startDate="2021-01-01T00:00:00Z", endDate="2022-01-01T00:00:00Z", amount=0, currency="USD",
                 amountPrePaid=0, amountFirstBill=0, billingInterval=0, billingOffset=0, overageSurchargePercent=0,
                 commitmentFeeDescription="", commitmentUsageDescription="", overageDescription="", id=""):
        self.accountId = accountId
        self.billingPlanId = billingPlanId
        self.productIds = []
        if productIds:
            self.productIds = productIds
        if contractId:
            self.contractId = contractId
        self.startDate = startDate
        self.endDate = endDate
        self.amount = amount
        self.currency = currency
        self.amountPrePaid = amountPrePaid
        self.amountFirstBill = amountFirstBill
        self.overageSurchargePercent = overageSurchargePercent
        self.billingInterval = billingInterval
        self.billingOffset = billingOffset
        self.commitmentFeeDescription = commitmentFeeDescription
        self.commitmentUsageDescription = commitmentUsageDescription
        self.overageDescription = overageDescription
        self.id = id


class MeasurementData:
    def __init__(self, meterCode="", accountCode="", timestamp=datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                 ets=None, measure=None, who=None, where=None, what=None, metadata=None, id=""):
        self.meter = meterCode
        self.account = accountCode
        self.ts = timestamp
        if ets:
            self.ets = ets
        if measure:
            self.measure = measure
        if who:
            self.who = who
        if where:
            self.where = where
        if what:
            self.what = what
        if metadata:
            self.metadata = metadata
        self.uid = id

    def todict(self):
        return self.__dict__


class Measure(M3terAPI):
    class_url = "/measurements"

    def __init__(self, id=""):
        self.measurements = []
        self.id = id

    def send(self, measurementData):
        self.measurements = measurementData
        url = ingest_api_url + self.class_url
        payload = json.dumps(self.__dict__)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Sending', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

    def getMeasureForAgg(self, aggregationId, startDate, endDate, accountCode):
        url = root_api_url + self.class_url + "/aggregations/" + aggregationId + "?startDate=" + startDate + "&endDate=" + endDate + "&accountCode=" + accountCode
        payload = None
        response = executeAPI(action="GET", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Retrieving', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        if response.status_code == 504:
            print('Request timed out because of too much data.')
            return json.loads('{"values":[]}')
        else:
            return json.loads(response.text)

    def build(self, measurementData):
        self.measurements.append(measurementData)
        return self

    def todict(self):
        return self.__dict__


class LineItem(M3terAPI):
    class_url = "/bills"

    def __init__(self, id=""):
        self.id = id


class Bill(M3terAPI):
    class_url = "/bills"

    def __init__(self, id=""):
        self.id = id

    def getAccountBill(self, accountId):
        url = root_api_url + self.class_url + "/accountid/" + accountId
        print(url)
        payload = None
        response = executeAPI(action="GET", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Retrieving', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        result = json.loads(response.text)['data']
        # print(result)
        return result


class Billjob(M3terAPI):
    class_url = "/billjobs"

    def __init__(self, lastDateInBillingPeriod=None, targetCurrency='USD', currencyConversions=None, accountIds=None,
                 billingFrequency='MONTHLY', billFrequencyInterval=1, id=""):
        self.id = id
        self.targetCurrency = targetCurrency
        self.lastDateInBillingPeriod = lastDateInBillingPeriod
        self.billingFrequency = billingFrequency
        self.billFrequencyInterval = billFrequencyInterval
        self.currencyConversions = []
        if currencyConversions:
            for conversion in currencyConversions:
                self.currencyConversions.append(conversion)
        self.accountIds = None
        if accountIds:
            self.accountIds = []
            for account in accountIds:
                self.accountIds.append(account)


class Alert(M3terAPI):
    class_url = "/alerts"

    def __init__(self, id=""):
        self.id = id


class ExternalMapping(M3terAPI):
    class_url = "/externalmappings"

    def __init__(self, m3terEntity=None, m3terId=None, externalSystem=None, externalTable=None, externalId=None, id=""):
        self.id = id
        self.m3terEntity = m3terEntity
        self.m3terId = m3terId
        self.externalSystem = externalSystem
        self.externalTable = externalTable
        self.externalId = externalId


class OrganizationConfig(M3terAPI):
    class_url = "/organizationconfig"

    def __init__(self, timezone="UTC", yearEpoch="2021-01-01", monthEpoch="2021-01-01", weekEpoch="2021-01-04",
                 dayEpoch="2021-01-01", currency="USD", daysBeforeBillDue=14, scheduledBillInterval=0,
                 standingChargeBillInAdvance=False, commitmentFeeBillInAdvance=True, minimumSpendBillInAdvance=False,
                 id=""):
        self.id = id
        self.timezone = timezone
        self.yearEpoch = yearEpoch
        self.monthEpoch = monthEpoch
        self.weekEpoch = weekEpoch
        self.dayEpoch = dayEpoch
        self.currency = currency
        self.daysBeforeBillDue = daysBeforeBillDue
        self.scheduledBillInterval = scheduledBillInterval
        self.standingChargeBillInAdvance = standingChargeBillInAdvance
        self.commitmentFeeBillInAdvance = commitmentFeeBillInAdvance
        self.minimumSpendBillInAdvance = minimumSpendBillInAdvance

    def get(self):
        url = root_api_url + self.class_url
        payload = None
        response = executeAPI(action="GET", token=TOKEN, url=url, payload="")
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Getting', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)


class UsageData(M3terAPI):
    class_url = "/dataexplorer/usagedata"

    def __init__(self, id=None):
        self.id = id

    def query(self, query):
        url = root_api_url + self.class_url
        payload = json.dumps(query)
        response = executeAPI(action="POST", token=TOKEN, url=url, payload=payload)
        if LOGGING: logger.debug(logfile=logfile, entity=self.class_url[1:-1], action='Creating', payload=payload,
                             status=response.status_code, response=response.text, url=url)
        return json.loads(response.text)

from tempfile import NamedTemporaryFile, TemporaryFile
import logging
import json

#from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
#from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.salesforce_hook import SalesforceHook


class SalesforceBulkQueryToGCSOperator(BaseOperator):
    """
        Queries the Salesforce Bulk API using a SOQL stirng. Results are then
        put into an GCS Bucket.
    :param sf_conn_id:      Salesforce Connection Id
    :param soql:            Salesforce SOQL Query String used to query Bulk API
    :param: object_type:    Salesforce Object Type (lead, contact, etc)
    :param gcp_conn_id:     GCP Connection Id
    :param gcs_bucket:      GCS Bucket where query results will be put
    :param gcs_filename:    Filename to write to in
    """

    #template_fields = ('soql', 's3_key')

    def __init__(self,
                 sf_conn_id,
                 soql,
                 object_type,
                 gcp_conn_id,
                 gcs_bucket,
                 gcs_filename,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.soql = soql
        self.object_type = object_type
        self.gcp_conn_id = gcp_conn_id
        self.gcs_bucket = gcs_bucket
        self.gcs_filename = gcs_filename
        self.object = object_type[0].upper() + object_type[1:].lower()

    def execute(self, context):
        sf_conn = SalesforceHook(self.sf_conn_id)#.get_conn()

        logging.info(self.soql)
        query_results = sf_conn.make_query(self.soql)


        gcs = GoogleCloudStorageHook(
            self.gcp_conn_id)
            #delegate_to=self.delegate_to)

        # One JSON Object Per Line

        #query_results = json.dumps(query_results)

        query_results = [json.dumps(result, ensure_ascii=False) for result in query_results['records']]
        query_results = '\n'.join(query_results)

        logging.info(query_results)

        byte_object = bytes(query_results, 'utf-8')
        temp_file = NamedTemporaryFile(mode='w+b')
        temp_file.write(byte_object)
        # s3.load_string(
        #     query_results,
        #     self.s3_key,
        #     bucket_name=self.s3_bucket,
        #     replace=True)

        gcs.upload(
            bucket=self.gcs_bucket,
            object='test',
            filename=temp_file.name
            #object_name=self.gcs_filename, #The object name to set when uploading the file.
            #data=query_results, #The file's data as a string or bytes to be uploaded.
            #mime_type='text/csv'
            #gzip=self.gzip --best practice would be uncompressed
        )

def get_all_fields(sf_conn_id,object):
    #get SF hook
    sf_conn = SalesforceHook(sf_conn_id)#.get_conn()

    #get list of available fields for current SF object
    field_list = sf_conn.get_available_fields(object)
    return field_list

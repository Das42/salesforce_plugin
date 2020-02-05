#from tempfile import NamedTemporaryFile
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
        #query_results = sf_conn.bulk.__getattr__(self.object).query(self.soql)
        query_results = sf_conn.query(self.soql)

        gcs = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        # One JSON Object Per Line
        query_results = [json.dumps(result, ensure_ascii=False) for result in query_results]
        query_results = '\n'.join(query_results)

        # s3.load_string(
        #     query_results,
        #     self.s3_key,
        #     bucket_name=self.s3_bucket,
        #     replace=True)

        gcs.upload(
            bucket_name=self.bucket,
            object_name=self.gcs_filename, #The object name to set when uploading the file.
            data=query_results, #The file's data as a string or bytes to be uploaded.
            #mime_type='application/json',
            #gzip=self.gzip --best practice would be uncompressed
        )

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery


# Create custom pipeline options
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_arparse_args(cls, parser):
        parser.add_value_provider_argument('--input_project')
        parser.add_value_provider_argument('--input_dataset')
        parser.add_value_provider_argument('--input_view')
        parser.add_value_provider_argument('--output_table')
        parser.add_value_provider_argument('--output_dataset')
        parser.add_value_provider_argument('--output_project')
        parser.add_value_provider_argument('--output_file')
        parser.add_value_provider_argument('--output_bucket')


# Set pipeline options
pipeline_options = PipelineOptions()

# Pipeline definition
with beam.Pipeline(options=pipeline_options) as p:
    
    # create object containing our command line arguments
    custom_options = pipeline_options.view_as(CustomOptions)

    # Input query
    input_query = (f"SELECT * FROM `{custom_options.input_project}`." + 
                  f"{custom_options.input_dataset}.{custom_options.input_dataset}")

    # Output table definition
    out_table = bigquery.TableReference(
        projectId=custom_options.input_project,
        datasetId=custom_options.input_dataset,
        tableId=custom_options.input_table
    )

    # Output file name
    out_file = f"gs://{custom_options.target_bucket}/{custom_options.target_file}"

    # Pipeline execution steps
    query_results = p | "Execute query" >> beam.io.Read(beam.io.BigQuerySource(query=input_query,use_standard_sql=true))
    #export_to_bq = (
    #    query_results | "Write results to BigQuery"
    #)



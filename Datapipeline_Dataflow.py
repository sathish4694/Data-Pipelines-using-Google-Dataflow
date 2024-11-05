import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
import json


class FormatData(beam.DoFn):
    def process(self, element):
        from datetime import datetime, timezone
        data = json.loads(element)
        
        # Standardize and clean the data
        data['company_name'] = data.get('company_name', '').strip().upper()
        data['country'] = data.get('country', '').upper()
        data['industry'] = " ".join(word.capitalize() for word in data.get('industry', '').split())
        
        # Handle size range
        try:
            size_range = data.get('size', '0').split('-')
            data['size'] = max(map(int, size_range))
        except ValueError:
            data['size'] = 0
        
        # Add load_date
        data['load_date'] = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        
        # Extract city and state from headquarters
        headquarters = data.get('headquarters', '').split(',')
        data['city'] = headquarters[0].strip() if len(headquarters) > 0 else None
        data['state'] = headquarters[1].strip() if len(headquarters) > 1 else None
        

# Fill 'latest_news' if it is not available or is empty
        data['latest_news'] = data['latest_news'] if data.get('latest_news') not in (None, '') else 'Not available'

        
        # Remove unused fields
        data.pop('description', None)
        data.pop('locationUrl', None)
        data.pop('headquarters', None)
        
        return [(data['company_name'], data)]


class LoadCEOData(beam.DoFn):
    def process(self, element):
        import csv
        row = list(csv.reader([element]))[0]  # Read line as a list
        # CSV columns are 'company_name' and 'ceo'
        company_name = row[0].strip().upper()  # Adjust index based on the CSV format
        ceo_name = row[1].strip() if len(row) > 1 else 'Not available'
        yield (company_name, ceo_name)


class MergeCEOData(beam.DoFn):
    def process(self, element):
        company_name, grouped_data = element
        main_data_list = grouped_data.get('main', [])
        ceo_data_list = grouped_data.get('ceo', [])
        
        for main_data in main_data_list:
            # Assign CEO data if available, otherwise default to 'Not available'
            main_data['ceo'] = ceo_data_list[0] if ceo_data_list else 'Not available'
            yield main_data


class FilterFieldsForCSV(beam.DoFn):
    def process(self, element):
        subset = {
            'company_name': element.get('company_name'),
            'ceo': element.get('ceo', 'Not available'),
            'country': element.get('country'),
            'state': element.get('state'),
            'city': element.get('city'),
            'founded': element.get('founded', 'Unknown')
        }
        yield ','.join(map(str, subset.values()))


def run():
    pipeline_options = PipelineOptions()
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'gcpprofessionaldataengineer'
    google_cloud_options.region = 'us-east1'
    google_cloud_options.staging_location = 'gs://supplier_data_miracle/staging'
    google_cloud_options.temp_location = 'gs://supplier_data_miracle/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    table_schema = {
        'fields': [
            {'name': 'company_name', 'type': 'STRING'},
            {'name': 'location', 'type': 'STRING'},
            {'name': 'country', 'type': 'STRING'},
            {'name': 'industry', 'type': 'STRING'},
            {'name': 'website', 'type': 'STRING'},
            {'name': 'size', 'type': 'INTEGER'},
            {'name': 'ceo', 'type': 'STRING'},
            {'name': 'latest_news', 'type': 'STRING'},
            {'name': 'linkedin_url', 'type': 'STRING'},
            {'name': 'point_of_contact', 'type': 'STRING'},
            {'name': 'id', 'type': 'STRING'},
            {'name': 'specialties', 'type': 'STRING'},
            {'name': 'founded', 'type': 'STRING'},
            {'name': 'load_date', 'type': 'DATE'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'city', 'type': 'STRING'},
        ]
    }
    
    with beam.Pipeline(options=pipeline_options) as pipeline:
        main_data = (
            pipeline
            | 'Read Main Data' >> ReadFromText('gs://supplier_data_miracle/sample_suppliers_data.json')
            | 'Format Main Data' >> beam.ParDo(FormatData())
        )

        ceo_data = (
            pipeline
            | 'Read CEO Data' >> ReadFromText('gs://supplier_data_miracle/company_ceo_data.csv')
            | 'Parse CEO Data' >> beam.ParDo(LoadCEOData())
        )

        # Join and merge CEO data with main data
        merged_data = (
            {'main': main_data, 'ceo': ceo_data}
            | 'Join Main and CEO Data' >> beam.CoGroupByKey()
            | 'Merge CEO Data' >> beam.ParDo(MergeCEOData())
        )

        # Write merged data to BigQuery
        merged_data | 'Write to BigQuery' >> WriteToBigQuery(
            table='gcpprofessionaldataengineer:suppliers_data.test2',
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
        
        # Write merged data to CSV
        (merged_data
            | 'Filter Fields for CSV' >> beam.ParDo(FilterFieldsForCSV())
            | 'Write to CSV' >> WriteToText(
                'gs://supplier_data_miracle/output/suppliers_data_output_test2',
                file_name_suffix='.csv',
                header='company_name,ceo,country,state,city,founded'
            )
        )

if __name__ == '__main__':
    run()

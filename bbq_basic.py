import apache_beam as beam

# source: https://beam.apache.org/documentation/io/built-in/google-bigquery/

quotes_list = [{'source': 'Mahatma Gandhi', 'quote': 'My life is my message!!'},
               {'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'!!"}]

def run_a_simple_pipeline(project_id, wtbq_dict):
  
  beam_options = beam.options.pipeline_options.PipelineOptions()
  
  with beam.Pipeline(options=beam_options) as pipeline:
  
    # two_rows = (pipeline | 'QueryTable' >> beam.io.ReadFromBigQuery(
    #    query='SELECT id FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 2', method="EXPORT"))
    # print(two_rows)
    
    (pipeline | beam.Create(quotes_list)
              | "WriteToBigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(
                                                  table=f'{project_id}:dummy2_dataset.dummy2_table',
                                                  schema='source:STRING, quote:STRING', 
                                                  **wtbq_dict))
    
import datetime
import apache_beam as beam

posts_list = [{'id': 1, 'view_count': 2132, 'tags': '<image-generation><prompt-design>', 'creation_date': datetime.datetime(2023, 7, 17, 19, 24, 1)},
               {'id': 5, 'view_count': 459, 'tags': '<gpu><llm>', 'creation_date': datetime.datetime(2023, 7, 17, 21, 33, 5)}, 
               {'id': 7, 'view_count': 605, 'tags': '<gpt><terminology><poe.com>', 'creation_date': datetime.datetime(2023, 7, 18, 0, 16, 17)}, 
               {'id': 8, 'view_count': 432, 'tags': '<image><image-generation><clipdrop>', 'creation_date': datetime.datetime(2023, 7, 18, 3, 11, 25)}, 
               {'id': 9, 'view_count': 271, 'tags': '<chatgpt>', 'creation_date': datetime.datetime(2023, 7, 18, 8, 39, 49)}]

def run_a_posts_basic_pipeline(project_id, wtbq_dict):

  beam_options = beam.options.pipeline_options.PipelineOptions()

  with beam.Pipeline(options=beam_options) as pipeline:

    # two_rows = (pipeline | 'QueryTable' >> beam.io.ReadFromBigQuery(
    #    query='SELECT id FROM `bigquery-public-data.stackoverflow.posts_questions` LIMIT 2', method="EXPORT"))
    # print(two_rows)

    ( pipeline | beam.Create(posts_list)
               | "WriteToBigQuery" >> beam.io.gcp.bigquery.WriteToBigQuery(table=f'{project_id}:dummy2_dataset.dummy2_posts_basic_table',
                                                                           **wtbq_dict))

import apache_beam as beam
import xml.etree.ElementTree as element_tree
from datetime import datetime



table_schema = {
    'fields': [
        {'name' : 'id', 'type': 'INTEGER', 'mode': 'Required'},
        {'name' : 'view_count', 'type': 'INTEGER', 'mode': 'Required'},
        {'name' : 'tags', 'type': 'STRING', 'mode': 'Required'},
        {'name' : 'creation_date', 'type': 'TIMESTAMP', 'mode': 'Required'}
    ]
}

#table_schema = "id:INTEGER, view_count:INTEGER, tags:STRING, creation_date:TIMESTAMP"

def to_datetime(s):
  # '2023-07-17T19:24:01.893'
  result = datetime.strptime(s[:s.find('.')], '%Y-%m-%dT%H:%M:%S')
  return result

structure = [('Id', int, 'id'), ('ViewCount', int, 'view_count'),  ('Tags', str, 'tags'), ('CreationDate', to_datetime, 'creation_date') ]

def tree_elem_to_dict(el):

  missing = [s[0] for s in structure if s[0] not in el.attrib]
  if (el.tag != 'row') or missing:
    return None
  result = {}
  for tree_key, transform_func, result_key in structure:
    try:
      result[result_key] = transform_func(el.attrib[tree_key])
    except:
      result[result_key] = None
  return result

def parse_into_dict(filename):
    tree = element_tree.parse(filename)
    results = [tree_elem_to_dict(el) for el in tree.iterfind('row')]
    results = [r for r in results if r is not None]
    return results

def run_a_posts_pipeline(project_id, filename, wtbq_dict):
  beam_options = beam.options.pipeline_options.PipelineOptions()

  with beam.Pipeline(options=beam_options) as pipeline:
    ( pipeline | 'file_to_dict' >> beam.Create(parse_into_dict(filename))
               | 'tobq' >> beam.io.WriteToBigQuery(table=f'{project_id}:dummy2_dataset.dummy_posts_table',
                                                   schema=table_schema,                  
                                                   **wtbq_dict))

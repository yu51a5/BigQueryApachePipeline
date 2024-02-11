import apache_beam as beam
import xml.etree.ElementTree as element_tree
from datetime import datetime

def to_datetime(s):
  # '2023-07-17T19:24:01.893'
  result = datetime.strptime(s[:s.find('.')], '%Y-%m-%dT%H:%M:%S')
  return result

types_map = { int : 'INTEGER', str : 'STRING', datetime : 'TIMESTAMP'}

results_structure = [('Id', int, 'id'), ('ViewCount', int, 'view_count'),  ('Tags', str, 'tags'), ('CreationDate', datetime, 'creation_date') ]
results_table_schema = {'fields': [{'name' : col_name, 'type': types_map[col_type], 'mode': 'Required'} for _, col_type, col_name in results_structure] }

errors_structure = [(int, 'i'), (str, 'id'),  (str, 'missing_keys'), (str, 'wrong_type_keys') ]
errors_table_schema = {'fields': [{'name' : col_name, 'type': types_map[col_type], 'mode': 'Required'} for col_type, col_name in errors_structure] }

def result_or_error(inp, _):
  return 0 if 'i' not in inp else 1

def tree_elem_to_dict(i, el):

  result = {}
  missing_keys = []
  wrong_type_keys = []
  for tree_key, transform_type, result_key in results_structure:
    if tree_key not in el.attrib:
      missing_keys.append(tree_key)
      continue
    try:
      transform_func = to_datetime if transform_type is datetime else transform_type
      result[result_key] = transform_func(el.attrib[tree_key])
    except:
      wrong_type_keys.append([el.attrib[tree_key], transform_type, type(el.attrib[tree_key])])
  if (missing_keys or wrong_type_keys):
    return {'i' : i, 
            'id' : el.attrib['Id'] if 'Id' in el.attrib else 'unknown', 
            'missing_keys' : str(missing_keys), 
            'wrong_type_keys' : str(wrong_type_keys)}
  else:
    return result

def parse_into_dict(filename):
  tree = element_tree.parse(filename)
  results = [tree_elem_to_dict(i, el) for i, el in enumerate(tree.iterfind('row'))]
  return results

def run_a_posts_pipeline(project_id, filename, wtbq_dict):
  beam_options = beam.options.pipeline_options.PipelineOptions()

  with beam.Pipeline(options=beam_options) as pipeline:
    results, errors = ( pipeline | 'file_to_dict' >> beam.Create(parse_into_dict(filename))
                                 | 'Partition' >> beam.Partition(result_or_error, 2))
    
    results | 'tobq_results' >> beam.io.WriteToBigQuery(table=f'{project_id}:dummy2_dataset.dummy_posts_results',
                                                   schema=results_table_schema,                  
                                                   **wtbq_dict)

    errors  | 'tobq_errors'  >> beam.io.WriteToBigQuery(table=f'{project_id}:dummy2_dataset.dummy_posts_errors',
                                                             schema=errors_table_schema,                  
                                                             **wtbq_dict)

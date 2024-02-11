import copy
import xmltodict
import apache_beam as beam

def parse_into_dict(xmlfile):
  with open(xmlfile) as ifp:
      doc = xmltodict.parse(ifp.read())
      return doc

def cleanup(x):
  y = copy.deepcopy(x)
  if '@ShippedDate' in x['ShipInfo']: # optional attribute
      y['ShipInfo']['ShippedDate'] = x['ShipInfo']['@ShippedDate']
      del y['ShipInfo']['@ShippedDate']
  return y

def get_orders(doc):
  for order in doc['Root']['Orders']['Order']:
      yield cleanup(order)

table_schema = {
    'fields': [
        {'name' : 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'EmployeeID', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'OrderDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'RequiredDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name' : 'ShipInfo', 'type': 'RECORD', 'mode': 'NULLABLE', 'fields': [
            {'name' : 'ShipVia', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'Freight', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipAddress', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCity', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipRegion', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipPostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShipCountry', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name' : 'ShippedDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        ]},
    ]
}

def run_an_order_pipeline(project_id, wtbq_dict):
  beam_options = beam.options.pipeline_options.PipelineOptions()
  
  with beam.Pipeline(options=beam_options) as pipeline:
    (pipeline | 'files' >> beam.Create(['orders.xml'])
              | 'parse' >> beam.Map(lambda filename: parse_into_dict(filename))
              | 'orders' >> beam.FlatMap(lambda doc: get_orders(doc))
              | 'tobq' >> beam.io.WriteToBigQuery(table=f'{project_id}:dummy2_dataset.dummy_orders_table',
                                                  schema=table_schema, 
                                                  **wtbq_dict))

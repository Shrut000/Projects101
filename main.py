import glob
import pandas as pd
import xml.etree.ElementTree as ET
import datetime


log_file = 'log_file.txt'
transformed_file = 'transformed_data.csv'

def csv_function(file_path): 
    """To extract data from csv file."""
    csv_file = pd.read_csv(file_path)
    return csv_file

def json_function(file_path):
    """To extract data from json file."""
    json_file = pd.read_json(file_path)
    return json_file

def xml_function(file_path):
    """To extract data from xml file."""
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for child in root:
        row = {}
        for grandchild in child:
            row[grandchild.tag] = grandchild.text
        data.append(row)
    xml_file = pd.DataFrame(data)
    return xml_file

def main_function(file_path):
    """ To extract the data based on the file format."""
    if file_path.endswith('.csv'):
        return csv_function(file_path)
    elif file_path.endswith('.json'):
        return json_function(file_path)
    elif file_path.endswith('.xml'):
        return xml_function(file_path)
    else:
        raise ValueError('Unsupported file format')
    
def transform_data(data):
    """To convert the height to meters and weight to kilograms"""
    data["height"] = data["height"] * 0.0254
    data["weight"] = data['weight'] * 0.453592
    return data

def load_data(data, file_path):
    """To load data to csv file."""
    data.to_csv(file_path, index=False)
    
def log_file_msg(message):
    """To log the message."""
    with open(log_file, 'a') as f:
        f.write(f'{datetime.datetime.now()}: {message}\n')

def etl_process():
    log_file_msg('Extraction phase started')
    files = glob.glob('data/*')
    final_df = []
    for file in files:
        df = main_function(file)
        final_df.append(df)
    combined_data = pd.concat(final_df, ignore_index=True)
    log_file_msg('Extraction phase completed')

    log_file_msg('Transformation phase started')
    transformed_data = transform_data(combined_data)
    log_file_msg('Transformation phase completed')

    log_file_msg('Loading phase started')
    load_data(transformed_data, transformed_file)
    log_file_msg('Loading phase completed')

if __name__ == '__main__':
    etl_process()

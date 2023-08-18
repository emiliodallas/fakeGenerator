import apache_beam as beam
from datetime import datetime
import re


class FillEmptyValues(beam.DoFn):
    def __init_(self, column):
        self.column = column

    def process(self, element):
        # Split the CSV row into values
        values = element.split(';')
        
        # Replace empty values with zeros
        if self.column < len(values):
            values[self.column] = values[self.column] or '0.0'
        
        # Join the filled values back into a CSV row
        filled_row = ';'.join(values)
        
        return [filled_row]

def run_dataflow_pipeline(input_file, output_file):
    with beam.Pipeline() as p:
        p | 'Read Input' >> beam.io.ReadFromText(input_file) \
          | 'Fill Empty Values' >> beam.ParDo(FillEmptyValues()) \
          | 'Write Output' >> beam.io.WriteToText(output_file)

class changeDelimiter(beam.DoFn):
    def process(self, element):
        new_line = element.replace('\t', ';')
        return [new_line]
    
class ConvertDateFormat(beam.DoFn):
    def process(self, element):
        # Split CSV line into columns
        columns = element.split(';')
        
        # Convert date format (assuming it's in the first column)
        old_date_str = columns[0]
        old_date = datetime.strptime(old_date_str, '%d/%m/%Y')
        new_date_str = old_date.strftime('%Y-%m-%d')
        
        # Replace the date in the first column with the new format
        columns[0] = new_date_str
        
        # Join columns back into a CSV line
        new_line = ';'.join(columns)
        
        return [new_line]
    
class ConvertDateFormat2(beam.DoFn):
    def process(self, element):
        # Split CSV line into columns
        if element.startswith("Estacao;Data;Hora;Precipitacao;TempBulboSeco;TempBulboUmido;TempMaxima;TempMinima;UmidadeRelativa"):
            return [element]
        columns = element.split(';')
        
        # Convert date format (assuming it's in the first column)
        old_date_str = columns[0]
        old_date = datetime.strptime(old_date_str, '%d/%m/%Y')
        new_date_str = old_date.strftime('%Y-%m-%d')
        
        # Replace the date in the first column with the new format
        columns[0] = new_date_str
        
        # Join columns back into a CSV line
        new_line = ';'.join(columns)
        
        return [new_line]
    
class CleanDoublePeriods(beam.DoFn):
    def process(self, element):
        # Split CSV line into columns
        columns = element.split(';')
        
        # Specify the index of the column you want to clean
        column_index_to_clean = 18  # Replace with the correct column index
        
        # Clean the specified column
        value_to_clean = columns[column_index_to_clean]
        cleaned_value = re.sub(r'[^\d.]', '', value_to_clean)  # Remove non-numeric characters except periods
        columns[column_index_to_clean] = cleaned_value
        
        # Join columns back into a CSV line
        new_line = ';'.join(columns)
        
        return [new_line]
    
class RemoveColumns(beam.DoFn):
    def process(self, element):
        # Split CSV line into columns
        columns = element.split(';')
        
        # Specify the indices of the columns to remove
        columns_to_remove = [2, 3, 4, 5, 6]  # Replace with the correct column indices
        
        # Remove the specified columns
        columns = [col for idx, col in enumerate(columns) if idx not in columns_to_remove]
        
        # Join columns back into a CSV line
        new_line = ';'.join(columns)
        
        return [new_line]
    
class ChangeColumnValues(beam.DoFn):
    def process(self, element):
        # Split CSV line into columns
        columns = element.split(';')
        
        # Specify the index of the column you want to change
        column_index_to_change = 0  # Replace with the correct column index
        
        # Change the values in the specified column
        value_to_change = columns[column_index_to_change]
        new_value = value_to_change[-2:]  # Extract the last two characters
        columns[column_index_to_change] = new_value
        
        # Join columns back into a CSV line
        new_line = ';'.join(columns)
        
        return [new_line]
    
class JoinAndReplace(beam.DoFn):
    def process(self, element, state_dict):
        code, *data = element
        state = state_dict.get(code, "Unknown")
        return [(state, *data)]

def run_dataflow_pipeline(input_file1, input_file2, output_file):
    with beam.Pipeline() as p:
        state_dict = (
            p | 'Read State Code Table' >> beam.io.ReadFromText(input_file1)
              | 'Split State Code' >> beam.Map(lambda line: line.strip().split(';'))
              | 'Create State Dictionary' >> beam.Map(lambda parts: (parts[1], parts[0]))
        )
        
        data = (
            p | 'Read Data Table' >> beam.io.ReadFromText(input_file2)
              | 'Split Data' >> beam.Map(lambda line: line.strip().split(';'))
        )
        
        joined_data = (
            data
            | 'Join and Replace' >> beam.ParDo(JoinAndReplace(), state_dict=beam.pvalue.AsDict(state_dict))
        )
        
        joined_data | 'Write Output' >> beam.io.WriteToText(output_file)

def run_dataflow_pipeline2(input_file, output_file, column):
    with beam.Pipeline() as p:
        p | 'Read Input' >> beam.io.ReadFromText(input_file) \
          | 'Change Delimiter' >> beam.ParDo(changeDelimiter()) \
          | 'Fill Empty Values' >> beam.ParDo(FillEmptyValues(column=column)) \
          | 'Convert Data' >> beam.ParDo(ConvertDateFormat()) \
          | 'Write Output' >> beam.io.WriteToText(output_file)
        


if __name__ == '__main__':

   # run_dataflow_pipeline(input_file='gs://covid-bucketo/soybitch/production.csv', output_file='gs://covid-bucketo/soybitch/treated/production.csv')

    #run_dataflow_pipeline(input_file='gs://covid-bucketo/soybitch/yield.csv', output_file='gs://covid-bucketo/soybitch/treated/yield.csv')

    #run_dataflow_pipeline2(input_file='gs://covid-bucketo/soybitch/conventional_weather_stations_inmet_brazil_1961_2019.csv', 
    #                       output_file='gs://covid-bucketo/soybitch/treated/conventional_weather_stations_inmet_brazil_1961_2019.csv')
    
    #run_dataflow_pipeline2(input_file='gs://covid-bucketo/soybitch/weather_stations_codes.csv', 
    #                       output_file='gs://covid-bucketo/soybitch/treated/weather_stations_codes.csv')

    #join_df_pipeline(input_file1='gs://covid-bucketo/soybitch/treated/weather_stations_codes.csv-00000-of-00001', 
    #                       input_file2='gs://covid-bucketo/soybitch/treated/conventional_weather_stations_inmet_brazil_1961_2019.csv-00000-of-00001',
    #                       output_file='gs://covid-bucketo/soybitch/treated/joinedWeather')


    run_dataflow_pipeline2(input_file='gs://covid-bucketo/soybitch/Soybean-Price.csv', 
                           output_file='gs://covid-bucketo/soybitch/treated/Soybean-Price.csv',
                           column=1)
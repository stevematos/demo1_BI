import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

import argparse

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

from beam_nuggets.io import relational_db

LENGTHCSV = 6

CONVERT_CANTIDAD = {
    'K': 1000,
    'M': 1000000
}


def view_records(record):
    print record


def get_clean_csv(valores):
    valores_clean = valores
    if len(valores) < LENGTHCSV:
        valores_clean = valores + [0] * (LENGTHCSV - len(valores))
    return valores_clean


def get_convertir_euro(valor):
    valor = valor[1:]
    cantidad = CONVERT_CANTIDAD.get(valor[-1], 1)

    valor_sin_cantidad = 0
    if valor[0:-1].strip():
        valor_sin_cantidad = float(valor[0:-1])

    return cantidad * valor_sin_cantidad


class SplitCsv(beam.DoFn):

    def process(self, element):
        # element = element[:-1]
        id, total, potencial, valor, salario, clausula_salida = get_clean_csv(element.split(','))

        return [{
            'id': int(id),
            'total': int(total),
            'potencial': int(potencial),
            'valor': get_convertir_euro(valor) if valor else 0,
            'salario': get_convertir_euro(salario) if salario else 0,
            'clausula_salida': get_convertir_euro(clausula_salida) if clausula_salida else 0,
            'tipo_moneda': 'euro'
        }]


class MapBigQueryRow(beam.DoFn):

    def __init__(self, key_column):
        super(MapBigQueryRow, self).__init__()
        self.key_column = key_column

    def process(self, element):
        # print (element)
        key = element.get(self.key_column)
        yield key, element


class TransformRow(beam.DoFn):

    def process(self, element):
        information = element[1]['information'][0]
        values = element[1]['values'][0]
        return [
            {
                'nombre': information['name'],
                'club': information['club'],
                'edad': information['age'],
                'nacionalidad': information['nationality'],
                'total_jugador': values['total'],
                'potencial_jugador': values['potencial'],
                'promedio_jugador': (values['potencial'] + values['total']) / 2,
                'valor': values['valor'],
                'valor_por_punto': (values['valor'] / values['total']),
                'salario': values['salario'],
                'clausula_salida': values['clausula_salida'],
                'tipo_moneda': values['tipo_moneda']
            }
        ]


class FormatRow(beam.DoFn):

    def process(self, element):
        return [
            """%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s""" %
            (element['nombre'], element['club'], element['edad'], element['nacionalidad']
             , element['total_jugador'], element['potencial_jugador'], element['promedio_jugador']
             , element['valor'], element['valor_por_punto'], element['salario']
             , element['clausula_salida'], element['tipo_moneda'])
        ]


class ETL:

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def __get_csv__(self, extract_file):
        data_read = (
                self.pipeline
                | 'leyendo archivo' >> ReadFromText(extract_file)
                | 'Sacando data' >> beam.ParDo(SplitCsv())
        )

        return data_read

    def __get_database__(self, config):
        config_database = relational_db.SourceConfiguration(
            drivername=config['drivername'],
            host=config['host'],
            port=config['port'],
            username=config['username'],
            password=config['password'],
            database=config['database'],
        )

        data_read = (
                self.pipeline
                | "Leyendo filas de la db" >> relational_db.ReadFromDB(
            source_config=config_database,
            table_name=config['table'],
            query=config['query']
        )
        )

        return data_read

    def extract(self, extract_file, config):
        values_players = self.__get_csv__(extract_file)
        information_players = self.__get_database__(config)

        return values_players, information_players

    def transform(self, values_players, information_players):
        values_players = values_players | "poniendo id al los valores" >> beam.ParDo(
            MapBigQueryRow('id'))
        information_players = information_players | "poniendo id a la informacion" >> beam.ParDo(
            MapBigQueryRow('id'))

        records = ({'values': values_players,
                    'information': information_players} | 'group_by_id' >> beam.CoGroupByKey())

        records_transform = records | 'transformando filas filas' >> beam.ParDo(TransformRow())
        return records_transform

    def load(self, records_load, output_file):
        records_load_final = records_load \
                             | 'Indicando formato' >> beam.ParDo(FormatRow()) \
                             | 'Exportando a un nuevo formato' >> WriteToText(output_file,
                                                                              header="""nombres,club,edad,nacionalidad,total_jugador,potencial_jugador,""" +
                                                                                     """promedio_jugador,valor,valor_por_punto,salario,clausula_salida,tipo_moneda""")

    def process(self, extract_file, config, output_file):
        values_players, information_players = self.extract(extract_file, config)
        records_load = self.transform(values_players, information_players)
        self.load(records_load, output_file)
        self.pipeline.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input',
                        help='Input for the pipeline',
                        default='gs://storage-fifa/valores_players.csv')
    parser.add_argument('-o', '--output',
                        help='Output for the pipeline',
                        default='./players-final.txt')

    args, pipeline_args = parser.parse_known_args()

    extract_file_csv = args.input
    query_database = """
			select * from players
	"""
    config_database = {
        'drivername': 'postgresql',
        'host': '35.247.199.205',
        'port': 5432,
        'username': 'postgres',
        'password': 'secret',
        'database': 'fifa',
        'table': 'players',
        'query': query_database
    }
    output_file = args.output

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    etl = ETL(p)

    etl.process(extract_file_csv, config_database, output_file)


if __name__ == "__main__":
    main()

import pandas as pd


class ConvertCsvToParquet:

    def __init__(self, input_csv, output_parquet):
        self.input_csv = input_csv
        self.output_parquet = output_parquet

    # Este método converte os arquivos .csv em .parquet
    @classmethod
    def csv_to_parquet(cls, input_csv, output_parquet):
        # Carregar o CSV para um DataFrame
        df = pd.read_csv(input_csv, encoding="latin_1", sep=';')
        # Salvar o DataFrame como arquivo Parquet

        df.to_parquet(output_parquet, index=False)
        print(f'Arquivo {input_csv} convertido com sucesso!')

    # Este método consulta o conteúdo do arquivo .parquet
    # Utilizar apenas em testes
    @classmethod
    def read_and_display_parquet(cls, parquet_file):
        # Carregar o arquivo Parquet para um DataFrame
        df = pd.read_parquet(parquet_file)

        # Exibir o conteúdo do DataFrame
        print('Conteúdo do arquivo Parquet:')
        print(df)


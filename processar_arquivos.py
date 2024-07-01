import os
import time
import zipfile
import pyarrow.parquet as pq
from convert_csv_to_parquet import ConvertCsvToParquet
from CONSTANTES import RADICAL_FINAL_BF


class ProcessarArquivos:

    def __init__(self, diretorio_csv, diretorio_parquet,
                 diretorio_parquet_final):
        self.diretorio_csv = diretorio_csv
        self.diretorio_parquet = diretorio_parquet
        self.diretorio_parquet_final = diretorio_parquet_final

    @classmethod
    def descompactar_zips(cls, diretorio_csv):
        # Verificar se há arquivos .zip no diretório_csv
        arqs_diretorio = os.listdir(diretorio_csv)
        # Lista de arquivos zipados
        arqs_zip_diretorio = []

        # Itera sobre cada arquivo na pasta
        for zip_raw in arqs_diretorio:
            # Verifica se o arquivo é .csv, inicia com "2021" e contém
            # a string "BolsaFamilia_Pagamentos e se já existe"
            csv_file = f"{diretorio_csv}/{zip_raw[:-4]}.csv"
            if (zip_raw.endswith(".zip") and zip_raw.startswith(
                    "2021") and "BolsaFamilia_Pagamentos" in zip_raw and not
            os.path.isfile(csv_file)):
                arqs_zip_diretorio.append(zip_raw)
                print(f"O arquivo {zip_raw} foi adicionado à lista de "
                      f"descompactação!")
            else:
                if os.path.isfile(csv_file):
                    zip_old = diretorio_csv + "/" + zip_raw
                    os.remove(zip_old)


        if bool(arqs_zip_diretorio):
            for item in arqs_zip_diretorio:
                zip_file = diretorio_csv + "/" + item

                print("----------------------------------------------"
                      "---------------------------------")
                print(f"Arquivo {item} será descompactado - AGUARDE")
                print("----------------------------------------------"
                      "---------------------------------")

                # Extrai o conteúdo do arquivo ZIP
                try:
                    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                        zip_ref.extractall(diretorio_csv)
                        print("----------------------------------------------"
                              "---------------------------------")
                        print(f"Arquivo {item} descompactado com sucesso")
                        print("----------------------------------------------"
                              "---------------------------------")
                except zipfile.BadZipFile:
                    print(f"[!] Erro ao descompactar: {item}")
                else:
                    # Remove o arquivo ZIP se descompactado com sucesso
                    os.remove(zip_file)
        else:
            print(f"AVISO:\n"
                  f"[!] Não há novo arquivo .zip a ser descompactado em\n"
                  f"[!] {diretorio_csv}")
            print("=========================================================="
                  "=====================")

        print(f"[!] Método de descompactação de arquivos .zip concluído.\n"
              f"[!] Os arquivos .zip são removidos do diretório\n"
              f"[!] {diretorio_csv}")
        print("=============================================================="
              "=================")

    # Este método processa individualmente os arquivos do diretório,
    # transformando-os em .parquet
    @classmethod
    def processa_csv_to_parquet(cls, diretorio_csv, diretorio_parquet):

        try:
            ProcessarArquivos.descompactar_zips(diretorio_csv)
        except Exception as erro:
            print(f"[!] Erro na execução da função:\n"
                  f"[!] 'ProcessarArquivos.descompactar_zips(diretorio_csv)'\n"
                  f"[!] {erro}")

        # Lista para armazenar os nomes dos arquivos .csv
        listaarquivos_csv = []

        # Laço para percorrer todos os arquivos no diretório
        for csv_raw in os.listdir(diretorio_csv):
            # Verifica se o arquivo é .csv, inicia com "2021" e contém
            # a string "BolsaFamilia_Pagamentos e se já existe"
            parquet_file = f"{diretorio_parquet}/{csv_raw[:-4]}.parquet"

            if (csv_raw.endswith(".csv") and csv_raw.startswith(
                    "2021") and "BolsaFamilia_Pagamentos" in csv_raw
                    and not os.path.isfile(parquet_file)):
                # Adiciona o nome do arquivo à lista 'listaarquivos_csv'
                listaarquivos_csv.append(csv_raw)
                print(f"O arquivo {csv_raw} foi adicionado à lista de "
                      f"conversão para .parquet!")
            else:
                if os.path.isfile(parquet_file):
                    print(f"ATENÇÃO!\n"
                          f"[!] O arquivo abaixo já existe no destino:"
                          f"[!] {parquet_file}")
        print("-----------------------------------------------------------"
              "--------------------")

        if bool(listaarquivos_csv):

            # Converte os arquivos da lista 'listaarquivos_csv' em .parquet e
            # Armazena no diretório 'diretorio_parquet'
            for item_csv in listaarquivos_csv:

                print("----------------------------------------------"
                      "---------------------------------")
                print(f"Arquivo {item_csv} será convertido em .parquet - "
                      f"AGUARDE")
                print("----------------------------------------------"
                      "---------------------------------")

                radical, extensao = os.path.splitext(item_csv)
                item_csv = diretorio_csv + "/" + item_csv
                arq_parquet = diretorio_parquet + "/" + radical + ".parquet"
                # print(f"Nome do arquivo (sem extensão): {radical}")
                # print(f"Nome do arquivo Parquet: {arq_parquet}")
                # print(f"Nome do arquivo csv: {item_csv}")

                try:
                    ConvertCsvToParquet.csv_to_parquet(item_csv, arq_parquet)
                except Exception as erro:
                    print(f"[!] Erro na execução da função:\n"
                          f"[!] 'ConvertCsvToParquet.csv_to_parquet(item_csv,"
                          f" arq_parquet)'\n"
                          f"[!] {erro}")
        else:
            print(f"AVISO:\n"
                  f"[!] Conteúdo do diretório\n"
                  f"[!] {diretorio_parquet}\n")
            arquivos_parquet = os.listdir(diretorio_parquet)
            for arq_parquet in arquivos_parquet:
                print(arq_parquet)
            print("----------------------------------------------"
                      "---------------------------------")

    # Esta função une todos os arquivos .parquet do diretório
    @classmethod
    def unir_parquet_files(cls, diretorio_csv, diretorio_parquet,
                           diretorio_parquet_final):

        inicio = time.time()

        try:
            ProcessarArquivos.processa_csv_to_parquet(diretorio_csv,
                                                      diretorio_parquet)
        except Exception as erro:
            print(f"[!] Erro na execução da função:\n"
                  f"[!] 'ProcessarArquivos.processa_csv_to_parquet("
                  f"diretorio_csv)'\n"
                  f"[!] {erro}")

        print("++++++++++++++++++++++++++++++++++++++++++++++"
              "+++++++++++++++++++++++++++++++++")
        print("Criação do Arquivo Final")
        print("++++++++++++++++++++++++++++++++++++++++++++++"
              "+++++++++++++++++++++++++++++++++")
        print(f"O arquivo final será gravado em:\n"
              f"{diretorio_parquet_final}\n"
              f"com o nome:\n"
              f"{RADICAL_FINAL_BF}.parquet - AGUARDE!")
        print("++++++++++++++++++++++++++++++++++++++++++++++"
              "+++++++++++++++++++++++++++++++++")

        # Lista para armazenar os nomes dos arquivos .csv
        arqs_parquet = []

        # Laço para percorrer todos os arquivos no diretório
        for arquivo in os.listdir(diretorio_parquet):
            # Verifica se o arquivo é .parquet
            if arquivo.endswith(".parquet"):
                # Adiciona o nome do arquivo à lista
                arqs_parquet.append(arquivo)

        if bool(arqs_parquet):

            # Nome do arquivo .parquet finalizado
            arq_parquet_final = (diretorio_parquet_final + "\\" +
                                 RADICAL_FINAL_BF + ".parquet")

            #print(arqs_parquet)
            #path e nome do primeiro arquivo .parquet para buscar o schema
            primeiro_arquivo = diretorio_parquet + "/" + arqs_parquet[0]

            schema = pq.ParquetFile(primeiro_arquivo).schema_arrow

            with pq.ParquetWriter(arq_parquet_final, schema=schema) as writer:
                for arquivo_parquet in arqs_parquet:
                    item_raw = diretorio_parquet + "/" + arquivo_parquet
                    writer.write_table(pq.read_table(item_raw, schema=schema))
            print("++++++++++++++++++++++++++++++++++++++++++++++"
                  "+++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++"
                  "+++++++++++++++++++++++++++++++++")
            print(f"Arquivo Final {RADICAL_FINAL_BF}.parquet criado com "
                  f"sucesso.")
            print("++++++++++++++++++++++++++++++++++++++++++++++"
                  "+++++++++++++++++++++++++++++++++")
            print("++++++++++++++++++++++++++++++++++++++++++++++"
                  "+++++++++++++++++++++++++++++++++")
        else:
            print(f"AVISO:\n"
                  f"[!] Não foi encontrado arquivo .parquet em\n"
                  f"[!] {diretorio_parquet},\n"
                  f"[!] que inicie por '2021' e que contenha "
                  f"a string 'BolsaFamilia_Pagamentos'\n"
                  f"[!] A lista de arquivos .parquet não foi criada!")
            print("=========================================================="
                  "=====================")

        fim = time.time()

        tempo_total = fim - inicio
        print(f"Tempo de execução: {tempo_total} segundos")
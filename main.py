from processar_arquivos import ProcessarArquivos

import os

dir_bf = os.path.dirname(__file__)
dir_bf_csv = os.path.join(dir_bf, 'arquivos/bf_csv')
dir_bf_parquet = os.path.join(dir_bf, 'arquivos/bf_parquet')

dir_parquet_final = os.path.join(dir_bf, 'arquivos/bf_parquet_final')

diretorios = [dir_bf_csv, dir_bf_parquet, dir_parquet_final]

if __name__ == "__main__":

    print("=============================================================="
          "=================")
    print("Início da execução do Programa!")
    print("=============================================================="
          "=================")

    for diret in diretorios:
        if not os.path.isdir(diret):
            raise TypeError(f"ATENÇÃO!!!!! O diretório {diret} não existe!")

    # ProcessarArquivos.descompactar_zips(dir_bf_csv)
    ProcessarArquivos.unir_parquet_files(dir_bf_csv, dir_bf_parquet,
                                         dir_parquet_final)
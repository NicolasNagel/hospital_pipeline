import os
import pandas as pd
import shutil
import logging

from typing import List, Dict
from datetime import datetime
from collections import defaultdict
from pathlib import Path

from src.cloud.cloud_connection import AzureCloud
from src.database.db_connection import DataBase

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

class Controller:
    """Responsável por fazer o Controle das Pipelines."""

    def __init__(self) -> None:
        self.cloud = AzureCloud()
        self.db = DataBase()
        self.download_path = 'src/temp_downloads'

    def start(self) -> None:
        """Inicia a Pipeline de Dados."""
        logger.info('Iniciando Pipeline de Dados...')

        start_time = datetime.now()
        try:
            self.db.drop_tables()
            self.db.create_tables()

            data = self.extract_data_from_cloud()
            data = self.transform_data()
            data = self.save_data_into_db_using_pandas(data)

            shutil.rmtree(Path(self.download_path))

            end_time = datetime.now()
            pipeline_time = (end_time - start_time).total_seconds()

            logger.info(f'Pipeline concluída com sucesso em {pipeline_time:.2f}s')

        except Exception as e:
            logger.error(f'Erro ao rodar a pipeline de dados: {str(e)}')
            raise

    def extract_data_from_cloud(self) -> None:
        """
        Extrai os dados da cloud e salva localmente temporariamente.

        Returns:
            None: Quantidade de arquivos salvos, se erro, mensagem de erro.
        """
        logger.info('Extraindo Dados da Cloud...')

        try:
            blob_file = self.cloud.list_blob_files()
            files = self._get_cloud_data(blob_file)

            temp_dir = Path(self.download_path)
            temp_dir.mkdir(exist_ok=True)

            for prefix, blob_file in files.items():
                file = prefix.split('_')[0]
                file_name = f'{file}.parquet'
                download_path = temp_dir / file_name

                data = self.cloud.download_data(
                    blob_name=blob_file
                )

                with open(download_path, 'wb') as file:
                    file.write(data)

                logger.info(f'"{download_path}" arquivo salvo com sucesso.')
        
        except Exception as e:
            logger.error(f'Erro ao extrair dados da cloud: {str(e)}')
            raise

    def transform_data(self) -> Dict[str, pd.DataFrame]:
        """
        Lista os arquivos dentro do diretório temporario e salva em um dicionário
        
        Returns:
            Dict(str, pd.DataFrame): Dicionário com {'nome do arquivo': pd.DataFrame}.
        """
        logger.info('Iniciando Transformação de Dados...')

        data = {}
        try:
            file_path = os.listdir(Path(self.download_path))

            for file in file_path:
                prefix = Path(file).stem
                full_path = os.path.join(self.download_path, file)
                df = pd.read_parquet(full_path)
                data[prefix] = df

                logger.info(f'{prefix} arquivo transformado com sucesso.')

            logger.info(f'{len(data)} arquivos transformados com sucesso.')
            return data
        
        except Exception as e:
            logger.error(f'Erro ao transformar arquivos: {str(e)}')
            raise

    def save_data_into_db(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        """
        Salva os dados no Banco de Dados.
        
        Args:
            df_dict (Dict[str, pd.DataFrame]): Dicionário com {'nome do arquivo': pd.DataFrame}.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Upload de Dados no Banco...')

        try:
            self.db.insert_data(df_dict)
            logger.info('Dados Inseridos no Banco.')

        except Exception as e:
            logger.error(f'Erro ao inserir dados no banco: {str(e)}')
            raise

    def _extract_timestamp(self, filename: str) -> datetime:
        """
        Pega o nome de um arquivo e retorna a data e a hora.
        
        Args:
            filename (str): Nome do arquivo (ex: 'encounters_2026-01-17T22:02:19.parquet').

        Returns:
            datetime: Data e Hora do arquivo (ex: '2026-01-17 22:02:19').
        """
        file_name = Path(filename).stem
        match = file_name.split('_')[-1]
        return datetime.fromisoformat(match)

    def _get_cloud_data(self, files: List[Path]) -> Dict[str, str]:
        """
        Pega o último arquivo de uma lista baseando no timestamp.
        
        Args:
            files (List[Path]): Lista com o diretório dos arquivos.

        Returns:
            Dict(str, str): Dicionário com {'prefixo do arquivo' : 'último arquivo salvo na Azure'}.
        """
        file_by_prefix = defaultdict(list)

        try:
            for file in files:
                prefix = Path(file).stem.split('/')[0]
                file_by_prefix[prefix].append(file)

            data = {}
            for name, content in file_by_prefix.items():
                last_date = max(content, key=lambda x: self._extract_timestamp(x))
                data[name] = last_date

            return data

        except Exception as e:
            logger.error(f'Erro ao tentar retornar o último arquivo: {str(e)}')
            raise

    def save_data_into_db_using_pandas(self, data: Dict[str, pd.DataFrame]) -> None:
        logger.info('Iniciando Insert de Dados no Banco...')

        if not data or data is None:
            logger.warning('Cancelando processo. Nenhum dado foi passado,')
            raise

        try:
            self.db.insert_data_with_pandas(data)
            logger.info('Processo concluído com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao concluir processo: {str(e)}')
            raise
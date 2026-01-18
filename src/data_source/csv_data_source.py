import os
import io
import pandas as pd
import logging

from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Optional
from pathlib import Path

from src.contracts.schema import EncontersSchema, OrganizationsSchema, PatientsSchema, PayersSchema, ProceduresSchema
from src.cloud.cloud_connection import AzureCloud

logger = logging.getLogger(__name__)

class DataSource:
    """Responsável por fazer Coleta de Dados do tipo CSV."""
    def __init__(self, cloud_conn: Optional[AzureCloud] = None):
        load_dotenv()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        )

        self.cloud_conn = cloud_conn or AzureCloud()
        self.file_path = 'src/data'
        self.validation_schema = {
            'encounters': EncontersSchema,
            'organizations': OrganizationsSchema,
            'patients': PatientsSchema,
            'payers': PayersSchema,
            'procedures': ProceduresSchema
        }

    def start(self):
        pass

    def extract_data(self) -> List[Path]:
        """
        Extrai os arquivos no diretório padrão e joga para uma lista.
        
        Returns:
            List(Path): Lista com os arquivos no diretório padrão.
        """
        logger.info('Iniciando Coleta de Dados...')

        files = []
        try:
            files_list = os.listdir(self.file_path)
            for file in files_list:
                if file.endswith('.csv'):
                    full_path = os.path.join(self.file_path, file)
                    files.append(full_path)

            logger.info(f'{len(files)} arquivos extraidos com sucesso.')
            return files
        
        except Exception as e:
            logger.error(f'Erro ao extrair arquivos: {str(e)}')
            return []
    
    
    def transform_data(self, data: List[Path]) -> Dict[str, pd.DataFrame]:
        """
        Transforma a lista com os diretórios em um dicionário.
        
        Args:
            data (List[Path]): Lista com os arquivos no diretório padrão.

        Returns:
            Dict(str, DataFrame): Dicionário com 'nome do arquivo': pd.DataFrame.
        """
        logger.info('Iniciando Transformação de Dados...')

        if not data or data is None:
            logger.warning('Transformação Cancelada. Nenhum arquivo foi passado')
            return {}
        
        df_dict = {}
        try:
            for file in data:
                filename = Path(file).stem
                logger.info(f'{filename}')
                df = pd.read_csv(file)

                df.columns = (
                    df.columns
                        .str.strip()
                        .str.lower()
                        .str.replace(' ', '_')
                        .str.replace('-', '_')
                )

                schema = self.validation_schema.get(filename)
                if not schema:
                    raise ValueError(f'Schema não encontrado para: {filename}')
                
                df_validated = schema.validate(df)
                df_dict[filename] = df_validated

            logger.info(f'{len(df_dict)} arquivos transformados com sucesso.')
            return df_dict
        
        except Exception as e:
            logger.error(f'Erro ao transformar dados: \n{str(e)}')
            return {}
    
    def load_data(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        """
        Transforma os arquiuvos em parquet e depois salva na Azure.
        
        Args:
            df_dict (Dict[str, DataFrame]): Dicionário com 'nome do arquivo': pd.DataFrame.
        
        Returns:
            None: Mensagem de sucesso, se erro, mensgagem de erro.
        """
        logger.info('Preparando Dados para serem salvos...')

        if not df_dict or df_dict is None:
            logger.warning('Salvamento cancelado. Nenhum dado foi passado.')
            raise

        try:
            for name, df in df_dict.items():
                download_buffer = io.BytesIO()
                df.to_parquet(download_buffer, engine='pyarrow', index=False)
                parquet_data = download_buffer.getvalue()

                file_name = self._rename_file()
                blob_name = f'{name}/{name}_{file_name}'

                self.cloud_conn.upload_data(blob_name=blob_name, data=parquet_data)
                logger.info(f'{blob_name} arquivo salvo com sucesso.')

            logger.info(f'{len(df_dict)} arquivos salvos com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao tentar salvar os arquivos na Azure: {str(e)}')
            raise

    def _rename_file(self) -> str:
        """
        Renomeia o nome do arquivo para ter o timezone.

        Returns:
            str: Nome do arquivo.
        """
        date_time = datetime.now().isoformat()
        match = date_time.split('.')[0]
        return f'{match}.parquet'
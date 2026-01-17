import os
import pandas as pd
import pandera.pandas as pa
import logging

from dotenv import load_dotenv
from typing import List, Dict, Optional
from pathlib import Path

from src.contracts.schema import EncontersSchema, OrganizationsSchema, PatientsSchema, PayersSchema, ProceduresSchema

logger = logging.getLogger(__name__)

class DataSource:
    """Classe responsável por fazer Coleta de Dados do tipo CSV."""
    def __init__(self):
        load_dotenv()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        )

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
    
    def load_data(self):
        pass

if __name__ == '__main__':
    data_source = DataSource()
    files = data_source.extract_data()
    df_dict = data_source.transform_data(files)
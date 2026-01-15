import os
import pandas as pd
import logging

from dotenv import load_dotenv
from typing import List, Dict, Optional
from pathlib import Path

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
                df = pd.read_csv(file)
                df_dict[filename] = df

            logger.info(f'{len(df_dict)} arquivos transformados com sucesso.')
            return df_dict
        
        except Exception as e:
            logger.error(f'Erro ao transformar dados: {str(e)}')
            return {}
    
    def load_data(self):
        pass

if __name__ == '__main__':
    data_source = DataSource()
    files = data_source.extract_data()
    print(files)
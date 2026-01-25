import os
import logging
import pandas as pd

from typing import Dict, Optional
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.database.db_model import (
    Base,
    EncountersModel,
    OrganizationsModel,
    PatientsModel,
    PayersModel,
    ProceduresModel
)

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)

class DataBase:
    """Responsável por fazer as conexões e as funções com o Banco de Dados."""

    def __init__(self):
        load_dotenv()

        self.db_user = os.getenv('DB_USER')
        self.db_pass = os.getenv('DB_PASS')
        self.db_host = os.getenv('DB_HOST')
        self.db_port = os.getenv('DB_PORT')
        self.db_name = os.getenv('DB_NAME')

        try:
            self.engine = create_engine(
                f'postgresql://{self.db_user}:{self.db_pass}@{self.db_host}:{self.db_port}/{self.db_name}',
                echo=False,
                pool_pre_ping=True,
                pool_size=10,
                max_overflow=20
            )

            self._Session = sessionmaker(bind=self.engine, autocommit=False, autoflush=False)
            self.Base = Base

        except Exception as e:
            logger.error(f'Erro ao se conectar ao Banco de Dados: {str(e)}')
            raise

        self.ORM_MAPPING = {
            'encounters': 'raw_encounters',
            'organizations': 'raw_organizations',
            'patients': 'raw_patients',
            'payers': 'raw_payers',
            'procedures': 'raw_procedures'
        }

    def create_tables(self) -> None:
        """Cria as tabelas no Banco de Dados."""
        logger.info('Criando as Tabelas no Banco de Dados...')
        
        try:
            self.Base.metadata.create_all(self.engine)
            logger.info('Tabelas criadas com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao criar as tabelas: {str(e)}')
            raise

    def drop_tables(self) -> None:
        """Deleta todas as tabelas no Banco de Dados."""
        logger.warning('AVISO: Deletando TODAS as Tabelas...')

        try:
            self.Base.metadata.drop_all(self.engine)
            logger.info('Tabelas deletadas com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao deletar as tabelas: {str(e)}')
            raise

    def insert_data(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 5_000) -> None:
        """
        Insere os registros no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Arquivo com 'nome_do_arquivo': pd.DataFrame.
            batch_size (Optional[int]): Quantidade de registros para o Batch de Dados.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Inserção de Dados no Banco...')

        if not df_dict or df_dict is None:
            logger.warning('Inserção cancelada. Nenhum dado foi passado.')
            raise
        
        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                records = df.to_dict(orient='records')
                total_records = len(records)

                for i in range(0, total_records, batch_size):
                    batch = records[i:i + batch_size]
                    session.bulk_insert_mappings(model, batch)

                    records_inserted = min(i + batch_size, total_records)
                    logger.info(f'{records_inserted}/{total_records} arquivos inseridos.')

                logger.info(f'{total_records} inseridos para: {name}')

            logger.info('Inserção concluída com sucesso.')
            session.commit()

        except Exception as e:
            logger.error(f'Erro ao inserir dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def update_data(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 5_000) -> None:
        """
        Atualiza os registros no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Arquivo com 'nome_do_arquivo': pd.DataFrame.
            batch_size (Optional[int]): Quantidade de registros para o Batch de Dados.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Atualizando arquivos no Banco...')

        if not df_dict or df_dict is None:
            logger.warning('Atualização cancelada. Nenhum dado foi passado')
            raise

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                records = df.to_dict(orient='records')
                total_records = len(records)

                for i in range(0, total_records, batch_size):
                    batch = records[i:i + batch_size]
                    session.bulk_update_mappings(model, batch)

                    records_inserted = min(i + batch_size, total_records)
                    logger.info(f'{records_inserted}/{total_records} registros atualizados.')

                logger.info(f'{total_records} arquivos inseridos para: {name}')

            logger.info('Tabelas atualizadas com sucesso.')
            session.commit()

        except Exception as e:
            logger.error(f'Erro ao atualizar dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def upsert_data(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 5_000) -> None:
        """
        Atualiza ou faz o Insert dos registros no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Arquivo com 'nome_do_arquivo': pd.DataFrame.
            batch_size (Optional[int]): Quantidade de registros para o Batch de Dados.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Upsert de Dados...')

        if not df_dict or df_dict is None:
            logger.warning('Upsert Cancelado. Nenhum registro foi passado.')
            return

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                pk_column = 'id'
                records = df.to_dict(orient='records')

                existing_ids = set(
                    row[0] for row in session.query(getattr(model, pk_column)).all()
                )

                existing = []
                new_records = []

                for record in records:
                    pk_value = record.get(pk_column)
                    if pk_value not in existing_ids:
                        new_records.append(record)
                    else:
                        existing.append(record)

                    if existing:
                        for i in range(0, len(existing), batch_size):
                            batch = existing[i:i + batch_size]
                            session.bulk_update_mappings(model, batch)
                        logger.info(f'{len(existing)} registros atualizados em: {name}')

                    if new_records:
                        for i in range(0, len(new_records), batch_size):
                            batch = new_records[i:i + batch_size]
                            session.bulk_insert_mappings(model, batch_size)
                        logger.info(f'{len(new_records)} registros salvos em: {name}')

                logger.info('Upsert concluído')
                session.commit()

        except Exception as e:
            logger.error(f'Erro ao fazer o upsert de dados: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def incremental_load(self, df_dict: Dict[str, pd.DataFrame], batch_size: Optional[int] = 5_000) -> None:
        """
        Insere apenas registros novos no Banco de Dados.
        
        Args:
            df_dict (Dict[str, DataFrame]): Arquivo com 'nome_do_arquivo': pd.DataFrame.
            batch_size (Optional[int]): Quantidade de registros para o Batch de Dados.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando atualização incremental...')

        if not df_dict or df_dict is None:
            logger.warning('Atualização incremental cancelada. Nenhum dado foi passado.')

        session = self._Session()

        try:
            for name, df in df_dict.items():
                model = self.ORM_MAPPING.get(name)
                pk_column = 'id'
                records = df.to_dict(orient='records')
                existing_ids = set(
                    row[0] for row in session.query(getattr(model, pk_column)).all()
                )

                new_records = [record for record in records if record.get(pk_column) not in existing_ids]
                
                if new_records:
                    for i in range(0, len(new_records), batch_size):
                        batch = new_records[i:i + batch_size]
                        session.bulk_insert_mappings(model, batch)

                        records_inserted = min(i + batch_size, len(new_records))
                        logger.info(f'{records_inserted}/{len(new_records)} registros adicionados...')

                    logger.info(f'{len(new_records)} registros inseridos para: {name}')
                else:
                    logger.info(f'Nenhum novo registro para: {name}')

            logger.info('Atualização incremental concluída.')
            session.commit()

        except Exception as e:
            logger.error(f'Erro ao fazer a atualizção incremental: {str(e)}')
            session.rollback()
            raise

        finally:
            session.close()

    def insert_data_with_pandas(self, df_dict: Dict[str, pd.DataFrame]) -> None:
        logger.info('Inserindo Dados...')

        if not df_dict or df_dict is None:
            logger.warning('Inserção cancelada. Nenhum dado foi passado.')
            raise

        try:
            for name, df in df_dict.items():
                table_name = self.ORM_MAPPING.get(name)
                df.to_sql(table_name, self.engine, if_exists='replace')
                logger.info(f'{len(df):.2f} linhas inseridas em {table_name}')

            logger.info(f'{len(df_dict)} Tabelas modificadas.')

        except Exception as e:
            logger.error(f'Erro ao inserir dados: {str(e)}')
            raise
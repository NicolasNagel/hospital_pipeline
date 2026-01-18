import os
import logging

from dotenv import load_dotenv
from typing import Optional, List
from pathlib import Path

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)

class AzureCloud:
    """
    Responsável por fazer as conexões com a Azure.
    """

    def __init__(self, container_name: Optional[str] = None):
        load_dotenv()

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
        )

        self.client_id = os.getenv('AZURE_CLIENT_ID')
        self.tenant_id = os.getenv('AZURE_TENANT_ID')
        self.client_secret = os.getenv('AZURE_CLIENT_SECRET')
        self.account_url = os.getenv('AZURE_ACCOUNT_URL')
        self.container_name = container_name or 'hospitaldata'

        try:
            self.credentials = ClientSecretCredential(
                client_id=self.client_id,
                tenant_id=self.tenant_id,
                client_secret=self.client_secret
            )

            self.blob_service_client = BlobServiceClient(
                account_url=self.account_url,
                credential=self.credentials
            )
        
        except Exception as e:
            logger.error(f'Erro ao se conectar com a Azure: {str(e)}')
            raise

    def upload_data(self, blob_name: str, data: bytes)-> None:
        """
        Faz o upload de arquivos na Azure.
        
        Args:
            blob_name (str): Nome do arquivo a ser salvo.
            data (bytes): Conteúdo do arquivo a ser salvo.

        Returns:
            None: Mensagem de sucesso, se erro, mensagem de erro.
        """
        logger.info('Iniciando Upload de Dados...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            blob_client.upload_blob(data=data, overwrite=True)
            logger.info(f'{blob_client} arquivo salvo com sucesso.')

        except Exception as e:
            logger.error(f'Erro ao fazer upload de arquivos: {str(e)}')
            raise

    def download_data(self, blob_name: str) -> bytes:
        """
        Faz o Download de arquivos da Azure.
        
        Args:
            blob_name: Nome do arquivo a ser baixado.

        Returns:
            bytes: Conteúdo binário do arquivo baixado.
        """
        logger.info('Iniciando Download de Arquivos...')

        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_name
            )

            download_buffer = blob_client.download_blob()
            data = download_buffer.readall()

            return data
        
        except Exception as e:
            logger.error(f'Erro ao fazer o download de arquivos: {str(e)}')
            raise

    def list_blob_files(self, blob_prefix: Optional[str] = None) -> List[Path]:
        """
        Lista os arquivos dentro do Container.
        
        Args:
            blob_prefix (Optional[str]): Nome do prefixo em que estão os arquivos (ex: 'raw_data', 'products').

        Returns:
            List(Path): Lista com o diretório completo dentro do Container (ex: 'products/products_2026/01/17')
        """
        logger.info('Listando Arquivos...')

        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)
            blobs = container_client.list_blobs(name_starts_with=blob_prefix)

            blob_name = [blob.name for blob in blobs]

            logger.info(f'{len(blob_name)} arquivos encontrados em: {blob_prefix}')
            return blob_name
        
        except Exception as e:
            logger.error(f'Erro ao listar arquivos: {str(e)}')
            raise
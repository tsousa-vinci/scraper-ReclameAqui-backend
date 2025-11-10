import pandas as pd
import io
import pickle
from datetime import datetime
from vincicompass import s3_manager as s3


def get_all_companies_from_s3(bucket_name='datascience-studies-files', file_key='repo_data/ReclameAqui/all_companies_full.pkl'):
    """
    Busca a base completa de reclamações do S3.
    
    Args:
        bucket_name (str): Nome do bucket S3
        file_key (str): Caminho do arquivo no bucket
        
    Returns:
        pd.DataFrame: DataFrame com todas as reclamações
    """
    print(f"📥 Buscando dados do S3: {bucket_name}/{file_key}")
    s3_client = s3.S3Manager(bucket_name)
    file_bytes = s3_client.get_file_bytes(file_key)
    df = pickle.loads(file_bytes)
    print(f"✅ {len(df)} registros carregados do S3")
    return df

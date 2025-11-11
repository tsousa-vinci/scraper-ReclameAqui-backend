import mongoengine
import sys
import os
from dotenv import load_dotenv
load_dotenv()

# Adicionar o diretório src ao path para importar os módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.db_connection import mongoDBConnection
from db.models.AllCompanies import AllCompanies
from ReclameAqui.collector import get_all_companies_from_s3



def main():
    """
    Script principal para atualizar a base de dados do ReclameAqui no MongoDB.
    
    Fluxo:
    1. Conecta ao MongoDB
    2. Busca dados completos do S3
    3. Faz atualização incremental (apenas novos registros)
    """
    try:
        # 1. Conectar ao MongoDB
        print("=" * 60)
        print("🚀 Iniciando atualização da base ReclameAqui")
        print("=" * 60)
        print("\n📡 Conectando ao MongoDB...")
        
        db_connection = mongoDBConnection("reclameAqui-db")
        db = db_connection.get_connection()
        mongoengine.connect(db=db.name, host=db_connection.connection_string)
        
        print("✅ Conectado ao MongoDB com sucesso!")
        
        # 2. Mostrar estatísticas atuais
        print("\n📊 Estatísticas atuais do MongoDB:")
        stats = AllCompanies.get_collection_stats()
        if stats and stats['total_records'] > 0:
            print(f"   Total de registros: {stats['total_records']:,}")
            print(f"   Data mais antiga: {stats['oldest_date']}")
            print(f"   Data mais recente: {stats['newest_date']}")
        else:
            print("   Base vazia (primeira execução)")
        
        # 3. Buscar dados do S3
        print("\n" + "=" * 60)
        df = get_all_companies_from_s3()
        
        # 4. Atualizar MongoDB incrementalmente
        print("\n" + "=" * 60)
        update_stats = AllCompanies.incremental_update_from_df(df)
        
        # 5. Mostrar estatísticas finais
        print("\n" + "=" * 60)
        print("📊 Estatísticas finais do MongoDB:")
        final_stats = AllCompanies.get_collection_stats()
        if final_stats:
            print(f"   Total de registros: {final_stats['total_records']:,}")
            print(f"   Data mais antiga: {final_stats['oldest_date']}")
            print(f"   Data mais recente: {final_stats['newest_date']}")
        
        print("\n" + "=" * 60)
        print("🎉 Atualização concluída com sucesso!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

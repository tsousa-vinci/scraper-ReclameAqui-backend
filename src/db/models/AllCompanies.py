import mongoengine
from mongoengine.errors import ValidationError, NotUniqueError
import pandas as pd
from datetime import datetime


class AllCompanies(mongoengine.Document):
    """
    Modelo para armazenar todas as reclamações do ReclameAqui.
    Atualização incremental baseada no campo 'id' (único) e 'created' (timestamp).
    """
    
    # Identificadores únicos
    id = mongoengine.StringField(required=True, unique=True, primary_key=True)
    oldComplainId = mongoengine.StringField()
    legacyId = mongoengine.StringField()
    
    # Timestamps
    created = mongoengine.DateTimeField(required=True)
    modified = mongoengine.DateTimeField()
    deletedDate = mongoengine.DateTimeField()
    firstInteractionDate = mongoengine.DateTimeField()
    
    # Informações da empresa
    companyName = mongoengine.StringField()
    companyShortname = mongoengine.StringField()
    fantasyName = mongoengine.StringField()
    company = mongoengine.StringField()
    empresa_origem = mongoengine.StringField()
    category = mongoengine.StringField()
    
    # Métricas da empresa
    company_finalScore = mongoengine.FloatField()
    company_consumerScore = mongoengine.FloatField()
    company_solvedPercentual = mongoengine.FloatField()
    company_dealAgainPercentual = mongoengine.FloatField()
    company_totalComplains = mongoengine.IntField()
    company_answeredPercentual = mongoengine.FloatField()
    company_index_type = mongoengine.StringField()
    
    # Conteúdo da reclamação
    title = mongoengine.StringField()
    titleMasked = mongoengine.StringField()
    description = mongoengine.StringField()
    descriptionMasked = mongoengine.StringField()
    
    # Classificação
    problemType = mongoengine.StringField()
    otherProblemType = mongoengine.StringField()
    productType = mongoengine.StringField()
    otherProductType = mongoengine.StringField()
    problema_categoria = mongoengine.StringField()
    
    # Status e avaliações
    status = mongoengine.StringField()
    solved = mongoengine.BooleanField()
    dealAgain = mongoengine.BooleanField()
    evaluation = mongoengine.StringField()
    evaluated = mongoengine.BooleanField()
    canBeEvaluated = mongoengine.BooleanField()
    score = mongoengine.FloatField()
    compliment = mongoengine.BooleanField()
    
    # Informações do usuário
    userName = mongoengine.StringField()
    requesterName = mongoengine.StringField()
    userEmail = mongoengine.StringField()
    userCity = mongoengine.StringField()
    userState = mongoengine.StringField()
    user = mongoengine.StringField()
    
    # Interações
    hasReply = mongoengine.BooleanField()
    lastReplyOrigin = mongoengine.StringField()
    interactions = mongoengine.ListField()  # Lista de dicts com histórico de interações
    
    # Moderação
    inModeration = mongoengine.BooleanField()
    moderateRequested = mongoengine.BooleanField()
    moderateReason = mongoengine.StringField()
    moderationReasonDescription = mongoengine.StringField()
    moderationUserName = mongoengine.StringField()
    maskingStatus = mongoengine.StringField()
    
    # Violações de política
    contentViolatesPolicies = mongoengine.BooleanField()
    contentPoliciesViolation = mongoengine.StringField()
    policiesViolationScore = mongoengine.FloatField()
    failedToValidatePolicies = mongoengine.DateTimeField()
    
    # Exclusão
    deleted = mongoengine.BooleanField()
    userRequestedDelete = mongoengine.BooleanField()
    deletionReason = mongoengine.StringField()
    deletedIp = mongoengine.StringField()
    
    # Outros
    type = mongoengine.StringField()
    presence = mongoengine.StringField()
    read = mongoengine.BooleanField()
    frozen = mongoengine.BooleanField()
    indexable = mongoengine.BooleanField()
    marketplaceComplain = mongoengine.BooleanField()
    publishedEmailSent = mongoengine.BooleanField()
    requestEvaluation = mongoengine.BooleanField()
    complainOrigin = mongoengine.StringField()
    url = mongoengine.StringField()
    ip = mongoengine.StringField()
    
    
    additionalFields = mongoengine.DictField()
    additionalInfo = mongoengine.StringField()  # Pode ser string vazia ou texto
    address = mongoengine.DictField()
    phones = mongoengine.ListField()
    files = mongoengine.ListField()
    companyIndexes = mongoengine.ListField()  # Lista de strings (JSON serializado)
    complainMediaInfos = mongoengine.ListField()
    raFormsAnswer = mongoengine.DictField()
    
    # Campos especiais
    count = mongoengine.IntField()
    Operadora = mongoengine.StringField()
    company_name = mongoengine.StringField()
    
    meta = {
        'collection': 'all_companies_full',
        'auto_create_index': False,
        'indexes': [
            {'fields': ['id'], 'unique': True},
            {'fields': ['-created']},  # Índice descendente para buscar mais recentes
            {'fields': ['companyShortname', '-created']},
            {'fields': ['status']},
            {'fields': ['created', 'companyShortname']},
        ]
    }

    @classmethod
    def get_last_updated_date(cls):
        """
        Retorna a data de criação do registro mais recente no MongoDB.
        Usado para identificar quais dados são novos.
        """
        try:
            last_doc = cls.objects.order_by('-created').first()
            if last_doc:
                return last_doc.created
            return None
        except Exception as e:
            print(f"⚠️ Erro ao buscar última data: {e}")
            return None

    @classmethod
    def incremental_update_from_df(cls, df):
        """
        Atualiza incrementalmente o MongoDB com novos dados do DataFrame.
        
        Estratégia:
        1. Busca a data mais recente no MongoDB
        2. Filtra apenas registros novos ou mais recentes do DataFrame
        3. Faz bulk upsert (insere novos, atualiza existentes)
        
        Args:
            df (pd.DataFrame): DataFrame com todos os dados do S3
            
        Returns:
            dict: Estatísticas da atualização
        """
        print("🔄 Iniciando atualização incremental...")
        
        try:
            # 1. Verificar última data no MongoDB
            last_date = cls.get_last_updated_date()
            
            if last_date is None:
                print("📦 MongoDB vazio. Inserindo todos os registros...")
                df_to_process = df.copy()
            else:
                print(f"📅 Última data no MongoDB: {last_date}")
                # Filtrar apenas registros mais recentes
                df_to_process = df[pd.to_datetime(df['created']) > last_date].copy()
                print(f"🆕 {len(df_to_process)} novos registros encontrados")
                
                if len(df_to_process) == 0:
                    print("✅ Base já está atualizada!")
                    return {
                        'new_records': 0,
                        'updated_records': 0,
                        'errors': 0,
                        'total_processed': 0
                    }
            
            # 2. Processar registros em lotes
            batch_size = 1000
            total_records = len(df_to_process)
            new_records = 0
            updated_records = 0
            errors = 0
            
            print(f"📊 Processando {total_records} registros em lotes de {batch_size}...")
            
            for i in range(0, total_records, batch_size):
                batch_df = df_to_process.iloc[i:i+batch_size]
                
                for idx, row in batch_df.iterrows():
                    try:
                        # Converter dados do DataFrame para dict
                        doc_data = cls._prepare_document_data(row)
                        
                        # Usar update_one com upsert=True para inserir ou atualizar
                        result = cls.objects(id=doc_data['id']).update_one(
                            upsert=True,
                            **doc_data
                        )
                        
                        if result == 0:
                            updated_records += 1
                        else:
                            new_records += 1
                            
                    except Exception as e:
                        errors += 1
                        if errors <= 5:  # Mostrar apenas os primeiros 5 erros
                            print(f"❌ Erro no registro {row.get('id', 'unknown')}: {e}")
                
                # Mostrar progresso
                processed = min(i + batch_size, total_records)
                print(f"   Processados: {processed}/{total_records} ({100*processed/total_records:.1f}%)")
            
            stats = {
                'new_records': new_records,
                'updated_records': updated_records,
                'errors': errors,
                'total_processed': total_records
            }
            
            print(f"\n✅ Atualização concluída!")
            print(f"   📥 Novos registros: {new_records}")
            print(f"   🔄 Registros atualizados: {updated_records}")
            print(f"   ❌ Erros: {errors}")
            
            return stats
            
        except Exception as e:
            print(f"❌ Erro na atualização incremental: {e}")
            raise

    @classmethod
    def _prepare_document_data(cls, row):
        """
        Prepara dados de uma linha do DataFrame para inserção no MongoDB.
        Converte tipos e trata valores nulos.
        """
        def safe_convert_datetime(val):
            """Converte para datetime, retorna None se inválido"""
            if pd.isna(val) or val is None:
                return None
            if isinstance(val, str):
                try:
                    return pd.to_datetime(val)
                except:
                    return None
            return val
        
        def safe_convert_bool(val):
            """Converte para bool, retorna None se inválido"""
            if pd.isna(val):
                return None
            return bool(val)
        
        def safe_convert_int(val):
            """Converte para int, retorna None se inválido"""
            if pd.isna(val):
                return None
            try:
                return int(val)
            except:
                return None
        
        def safe_convert_float(val):
            """Converte para float, retorna None se inválido"""
            if pd.isna(val):
                return None
            try:
                return float(val)
            except:
                return None
        
        def safe_convert_str(val):
            """Converte para string, retorna None se inválido"""
            if pd.isna(val) or val is None:
                return None
            return str(val)
        
        # Construir dicionário com todos os campos
        doc_data = {
            # Identificadores
            'id': safe_convert_str(row.get('id')),
            'oldComplainId': safe_convert_str(row.get('oldComplainId')),
            'legacyId': safe_convert_str(row.get('legacyId')),
            
            # Timestamps
            'created': safe_convert_datetime(row.get('created')),
            'modified': safe_convert_datetime(row.get('modified')),
            'deletedDate': safe_convert_datetime(row.get('deletedDate')),
            'firstInteractionDate': safe_convert_datetime(row.get('firstInteractionDate')),
            
            # Informações da empresa
            'companyName': safe_convert_str(row.get('companyName')),
            'companyShortname': safe_convert_str(row.get('companyShortname')),
            'fantasyName': safe_convert_str(row.get('fantasyName')),
            'company': safe_convert_str(row.get('company')),
            'empresa_origem': safe_convert_str(row.get('empresa_origem')),
            'category': safe_convert_str(row.get('category')),
            
            # Métricas da empresa
            'company_finalScore': safe_convert_float(row.get('company_finalScore')),
            'company_consumerScore': safe_convert_float(row.get('company_consumerScore')),
            'company_solvedPercentual': safe_convert_float(row.get('company_solvedPercentual')),
            'company_dealAgainPercentual': safe_convert_float(row.get('company_dealAgainPercentual')),
            'company_totalComplains': safe_convert_int(row.get('company_totalComplains')),
            'company_answeredPercentual': safe_convert_float(row.get('company_answeredPercentual')),
            'company_index_type': safe_convert_str(row.get('company_index_type')),
            
            # Conteúdo
            'title': safe_convert_str(row.get('title')),
            'titleMasked': safe_convert_str(row.get('titleMasked')),
            'description': safe_convert_str(row.get('description')),
            'descriptionMasked': safe_convert_str(row.get('descriptionMasked')),
            
            # Classificação
            'problemType': safe_convert_str(row.get('problemType')),
            'otherProblemType': safe_convert_str(row.get('otherProblemType')),
            'productType': safe_convert_str(row.get('productType')),
            'otherProductType': safe_convert_str(row.get('otherProductType')),
            'problema_categoria': safe_convert_str(row.get('problema_categoria')),
            
            # Status
            'status': safe_convert_str(row.get('status')),
            'solved': safe_convert_bool(row.get('solved')),
            'dealAgain': safe_convert_bool(row.get('dealAgain')),
            'evaluation': safe_convert_str(row.get('evaluation')),
            'evaluated': safe_convert_bool(row.get('evaluated')),
            'canBeEvaluated': safe_convert_bool(row.get('canBeEvaluated')),
            'score': safe_convert_float(row.get('score')),
            'compliment': safe_convert_bool(row.get('compliment')),
            
            # Usuário
            'userName': safe_convert_str(row.get('userName')),
            'requesterName': safe_convert_str(row.get('requesterName')),
            'userEmail': safe_convert_str(row.get('userEmail')),
            'userCity': safe_convert_str(row.get('userCity')),
            'userState': safe_convert_str(row.get('userState')),
            'user': safe_convert_str(row.get('user')),
            
            # Interações
            'hasReply': safe_convert_bool(row.get('hasReply')),
            'lastReplyOrigin': safe_convert_str(row.get('lastReplyOrigin')),
            # interactions é tratado como campo complexo (lista) abaixo
            
            # Moderação
            'inModeration': safe_convert_bool(row.get('inModeration')),
            'moderateRequested': safe_convert_bool(row.get('moderateRequested')),
            'moderateReason': safe_convert_str(row.get('moderateReason')),
            'moderationReasonDescription': safe_convert_str(row.get('moderationReasonDescription')),
            'moderationUserName': safe_convert_str(row.get('moderationUserName')),
            'maskingStatus': safe_convert_str(row.get('maskingStatus')),
            
            # Políticas
            'contentViolatesPolicies': safe_convert_bool(row.get('contentViolatesPolicies')),
            'contentPoliciesViolation': safe_convert_str(row.get('contentPoliciesViolation')),
            'policiesViolationScore': safe_convert_float(row.get('policiesViolationScore')),
            'failedToValidatePolicies': safe_convert_datetime(row.get('failedToValidatePolicies')),
            
            # Exclusão
            'deleted': safe_convert_bool(row.get('deleted')),
            'userRequestedDelete': safe_convert_bool(row.get('userRequestedDelete')),
            'deletionReason': safe_convert_str(row.get('deletionReason')),
            'deletedIp': safe_convert_str(row.get('deletedIp')),
            
            # Outros
            'type': safe_convert_str(row.get('type')),
            'presence': safe_convert_str(row.get('presence')),
            'read': safe_convert_bool(row.get('read')),
            'frozen': safe_convert_bool(row.get('frozen')),
            'indexable': safe_convert_bool(row.get('indexable')),
            'marketplaceComplain': safe_convert_bool(row.get('marketplaceComplain')),
            'publishedEmailSent': safe_convert_bool(row.get('publishedEmailSent')),
            'requestEvaluation': safe_convert_bool(row.get('requestEvaluation')),
            'complainOrigin': safe_convert_str(row.get('complainOrigin')),
            'url': safe_convert_str(row.get('url')),
            'ip': safe_convert_str(row.get('ip')),
            
            # Campos especiais
            'count': safe_convert_int(row.get('count')),
            'Operadora': safe_convert_str(row.get('Operadora')),
            'company_name': safe_convert_str(row.get('company_name')),
        }
        
        # Campos complexos - tratamento específico por tipo
        
        # additionalInfo é string, não dict
        val = row.get('additionalInfo')
        if pd.notna(val) and val is not None and val != '':
            doc_data['additionalInfo'] = safe_convert_str(val)
        
        # Dicts
        for field in ['additionalFields', 'address', 'raFormsAnswer']:
            val = row.get(field)
            if pd.notna(val) and val is not None:
                if isinstance(val, dict):
                    # Só adiciona se não for dict vazio
                    if val:
                        doc_data[field] = val
                elif isinstance(val, str):
                    # Tentar parsear JSON se for string
                    try:
                        import json
                        parsed = json.loads(val)
                        if parsed:
                            doc_data[field] = parsed
                    except:
                        pass
        
        # Listas (podem conter dicts, strings, etc)
        # interactions é especialmente importante - histórico de respostas/interações
        for field in ['interactions', 'phones', 'files', 'companyIndexes', 'complainMediaInfos']:
            val = row.get(field)
            if pd.notna(val) and val is not None:
                if isinstance(val, list):
                    # Só adiciona se não for lista vazia
                    if val:
                        doc_data[field] = val
                elif isinstance(val, str):
                    # Tentar parsear JSON se for string
                    try:
                        import json
                        parsed = json.loads(val)
                        if isinstance(parsed, list) and parsed:
                            doc_data[field] = parsed
                    except:
                        pass
        
        # Remover campos None (mongoengine não aceita None em update)
        doc_data = {k: v for k, v in doc_data.items() if v is not None}
        
        return doc_data

    @classmethod
    def get_collection_stats(cls):
        """
        Retorna estatísticas da coleção.
        """
        try:
            total = cls.objects.count()
            if total == 0:
                return {
                    'total_records': 0,
                    'oldest_date': None,
                    'newest_date': None
                }
            
            oldest = cls.objects.order_by('created').first()
            newest = cls.objects.order_by('-created').first()
            
            return {
                'total_records': total,
                'oldest_date': oldest.created if oldest else None,
                'newest_date': newest.created if newest else None
            }
        except Exception as e:
            print(f"❌ Erro ao buscar estatísticas: {e}")
            return None
            
        

import pandas as pd
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import re
from tqdm import tqdm
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnalisadorAgronegocio:
    def __init__(self):
        # Usa CPU por padrão, GPU se disponível
        self.device = 0 if torch.cuda.is_available() else -1

        # Carrega modelo mais sofisticado para português
        self.sentiment_pipeline = pipeline(
            "text-classification",
            model="cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
            device=self.device,
            return_all_scores=True
        )

        # Palavras-chave relacionadas ao agronegócio
        self.keywords_positivas = [
            'agricultura', 'pecuária', 'agronegócio', 'produção agrícola', 
            'tecnologia agrícola', 'sustentabilidade', 'produtividade',
            'inovação rural', 'desenvolvimento rural', 'economia rural',
            'segurança alimentar', 'exportação', 'commodities'
        ]

        self.keywords_negativas = [
            'desmatamento', 'poluição', 'agrotóxico', 'monocultura',
            'concentração fundiária', 'conflito agrário', 'degradação',
            'erosão', 'contaminação', 'exploração'
        ]

        # Padrões que indicam crítica ou apoio ao agronegócio
        self.padroes_critica = [
            r'\bagronegócio.*(?:destr|prejudic|problem|dano)',
            r'(?:contra|crítica).*agronegócio',
            r'agronegócio.*(?:ruim|mal|negativ)',
            r'fim.*agronegócio',
            r'pare.*agronegócio'
        ]

        self.padroes_apoio = [
            r'\bagronegócio.*(?:importante|essencial|fundamental)',
            r'(?:apoio|defendo).*agronegócio',
            r'agronegócio.*(?:bom|positiv|desenvolvimento)',
            r'viva.*agronegócio',
            r'sucesso.*agronegócio'
        ]

    def preprocessar_texto(self, texto):
        """Limpa e prepara o texto para análise"""
        if pd.isna(texto) or not isinstance(texto, str):
            return ""

        # Remove URLs, mentions e hashtags para análise de sentimento
        texto_limpo = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', texto)
        texto_limpo = re.sub(r'@\w+', '', texto_limpo)
        texto_limpo = re.sub(r'#\w+', '', texto_limpo)

        return texto_limpo.strip()

    def detectar_contexto_agronegocio(self, texto):
        """Detecta se o texto menciona agronegócio e em que contexto"""
        texto_lower = texto.lower()

        # Verifica se menciona agronegócio diretamente
        if 'agronegócio' not in texto_lower and 'agricultura' not in texto_lower and 'pecuária' not in texto_lower:
            return 'nao_relacionado'

        # Verifica padrões específicos de crítica
        for padrao in self.padroes_critica:
            if re.search(padrao, texto_lower):
                return 'critica_explicita'

        # Verifica padrões específicos de apoio
        for padrao in self.padroes_apoio:
            if re.search(padrao, texto_lower):
                return 'apoio_explicito'

        # Conta palavras-chave positivas e negativas
        palavras_positivas = sum(1 for palavra in self.keywords_positivas if palavra in texto_lower)
        palavras_negativas = sum(1 for palavra in self.keywords_negativas if palavra in texto_lower)

        if palavras_negativas > palavras_positivas:
            return 'contexto_negativo'
        elif palavras_positivas > palavras_negativas:
            return 'contexto_positivo'
        else:
            return 'contexto_neutro'

    def analisar_sentimento_geral(self, texto):
        """Analisa o sentimento geral do texto"""
        try:
            # Limita o texto para não exceder o limite do modelo
            texto_truncado = texto[:512]
            resultado = self.sentiment_pipeline(texto_truncado)[0]

            # Encontra o sentimento com maior confiança
            sentimento_principal = max(resultado, key=lambda x: x['score'])

            return {
                'label': sentimento_principal['label'],
                'score': sentimento_principal['score']
            }
        except Exception as e:
            logger.warning(f"Erro na análise de sentimento: {e}")
            return {'label': 'NEUTRAL', 'score': 0.0}

    def classificar_sentimento_agronegocio(self, texto):
        """Classifica especificamente a atitude em relação ao agronegócio"""
        texto_limpo = self.preprocessar_texto(texto)

        if not texto_limpo:
            return 'erro'

        # Detecta contexto
        contexto = self.detectar_contexto_agronegocio(texto_limpo)

        # Se não é relacionado ao agronegócio, retorna neutro
        if contexto == 'nao_relacionado':
            return 'neutro'

        # Se há crítica ou apoio explícito, retorna diretamente
        if contexto == 'critica_explicita':
            return 'negativo'
        elif contexto == 'apoio_explicito':
            return 'positivo'

        # Para casos ambíguos, usa análise de sentimento geral + contexto
        sentimento_geral = self.analisar_sentimento_geral(texto_limpo)

        if contexto == 'contexto_negativo':
            if sentimento_geral['label'] in ['NEGATIVE', 'negative']:
                return 'negativo'
            elif sentimento_geral['label'] in ['POSITIVE', 'positive'] and sentimento_geral['score'] > 0.7:
                return 'positivo'  # Sentimento muito positivo pode superar contexto negativo
            else:
                return 'negativo'  # Contexto negativo prevalece

        elif contexto == 'contexto_positivo':
            if sentimento_geral['label'] in ['POSITIVE', 'positive']:
                return 'positivo'
            elif sentimento_geral['label'] in ['NEGATIVE', 'negative'] and sentimento_geral['score'] > 0.7:
                return 'negativo'  # Sentimento muito negativo pode superar contexto positivo
            else:
                return 'positivo'  # Contexto positivo prevalece

        else:  # contexto_neutro
            if sentimento_geral['score'] > 0.6:
                if sentimento_geral['label'] in ['POSITIVE', 'positive']:
                    return 'positivo'
                elif sentimento_geral['label'] in ['NEGATIVE', 'negative']:
                    return 'negativo'
            return 'neutro'

def main():
    # Inicializa o analisador
    analisador = AnalisadorAgronegocio()

    # Lê o CSV original
    logger.info("Carregando dados...")
    df = pd.read_csv("data/bluesky_agronegócio_2025.csv")

    # Aplica análise de sentimento com barra de progresso
    logger.info("Iniciando análise de sentimento...")
    tqdm.pandas(desc="Analisando posts")
    df['sentiment_agronegocio'] = df['text'].progress_apply(
        analisador.classificar_sentimento_agronegocio
    )

    # Adiciona análise de contexto para debugging
    df['contexto_agronegocio'] = df['text'].apply(
        lambda x: analisador.detectar_contexto_agronegocio(
            analisador.preprocessar_texto(x)
        )
    )

    # Salva novo CSV
    logger.info("Salvando resultados...")
    df.to_csv("data/posts_com_sentimento_agronegocio.csv", index=False)

    # Exibe estatísticas detalhadas
    print("\n=== RELATÓRIO DE ANÁLISE ===")
    print(f"Total de posts analisados: {len(df)}")
    print("\nDistribuição de sentimentos em relação ao agronegócio:")
    print(df['sentiment_agronegocio'].value_counts())
    print(f"\nPercentuais:")
    percentuais = df['sentiment_agronegocio'].value_counts(normalize=True) * 100
    for sentiment, perc in percentuais.items():
        print(f"{sentiment}: {perc:.1f}%")

    print("\nDistribuição por contexto:")
    print(df['contexto_agronegocio'].value_counts())

    # Mostra alguns exemplos de cada categoria
    print("\n=== EXEMPLOS ===")
    for sentiment in ['positivo', 'negativo', 'neutro']:
        exemplos = df[df['sentiment_agronegocio'] == sentiment]['text'].head(2)
        if not exemplos.empty:
            print(f"\nExemplos de posts {sentiment}s:")
            for i, exemplo in enumerate(exemplos, 1):
                print(f"{i}. {exemplo[:150]}...")

if __name__ == "__main__":
    main()

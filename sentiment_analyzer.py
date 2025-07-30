from transformers import pipeline
import pandas as pd

# Carrega o pipeline com modelo multilíngue
sentiment_pipeline = pipeline("sentiment-analysis", model="nlptown/bert-base-multilingual-uncased-sentiment")

# Carrega os dados do seu CSV
df = pd.read_csv("bluesky_agronegócio_2025.csv")

# Limita os textos para no máximo 512 tokens e aplica o modelo
def get_sentiment_label(text):
    try:
        result = sentiment_pipeline(text[:512])[0]['label']
        # Converte para uma classificação simples
        if '1' in result or '2' in result:
            return 'negativo'
        elif '4' in result or '5' in result:
            return 'positivo'
        else:
            return 'neutro'
    except:
        return 'erro'

df['sentimento'] = df['text'].apply(get_sentiment_label)

# Conta os resultados
print(df['sentimento'].value_counts())

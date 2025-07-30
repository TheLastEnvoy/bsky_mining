import pandas as pd
from transformers import pipeline
import torch

# Usa CPU por padrão
device = 0 if torch.cuda.is_available() else -1

# Carrega o modelo multilingue
sentiment_pipeline = pipeline(
    "sentiment-analysis",
    model="nlptown/bert-base-multilingual-uncased-sentiment",
    device=device
)

# Lê o CSV original
df = pd.read_csv("bluesky_agronegócio_2025.csv")  # substitua com o nome real do seu arquivo

# Aplica análise de sentimento em lote
def classificar_sentimento(texto):
    try:
        resultado = sentiment_pipeline(texto[:512])[0]
        estrelas = int(resultado['label'][0])  # pega o número de estrelas (1 a 5)
        if estrelas <= 2:
            return 'negativo'
        elif estrelas == 3:
            return 'neutro'
        else:
            return 'positivo'
    except Exception as e:
        return 'erro'

# Aplica função a cada linha
df['sentiment'] = df['text'].apply(classificar_sentimento)

# Salva novo CSV
df.to_csv("posts_com_sentimento.csv", index=False)

# Exibe resumo
print(df['sentiment'].value_counts())

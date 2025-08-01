import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import re
from collections import Counter
import seaborn as sns

# Configuração melhorada do NLTK
def setup_nltk():
    """Configura os recursos necessários do NLTK com tratamento de erros."""
    try:
        import nltk

        # Lista de recursos necessários
        resources = ['punkt', 'stopwords', 'punkt_tab']

        for resource in resources:
            try:
                if resource == 'punkt':
                    nltk.data.find('tokenizers/punkt')
                elif resource == 'stopwords':
                    nltk.data.find('corpora/stopwords')
                elif resource == 'punkt_tab':
                    nltk.data.find('tokenizers/punkt_tab')
            except LookupError:
                print(f"Baixando recurso {resource}...")
                nltk.download(resource, quiet=True)

        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        return True, stopwords, word_tokenize

    except Exception as e:
        print(f"Erro ao configurar NLTK: {e}")
        print("Usando tokenização alternativa...")
        return False, None, None

# Configura NLTK
NLTK_AVAILABLE, stopwords_module, word_tokenize_func = setup_nltk()


class BlueskyWordCloudGenerator:
    """Gerador de nuvem de palavras para posts do Bluesky sobre agronegócio."""

    def __init__(self, excel_path: str):
        """
        Inicializa o gerador com o caminho da planilha Excel.

        Args:
            excel_path (str): Caminho para o arquivo Excel
        """
        self.excel_path = excel_path
        self.df = None
        self.processed_text = ""
        self.stop_words = self._get_extended_stopwords()

    def _get_extended_stopwords(self) -> set:
        """
        Cria lista expandida de stopwords em português.

        Returns:
            set: Conjunto de stopwords personalizadas
        """
        # Stopwords básicas em português (caso NLTK não esteja disponível)
        basic_portuguese_stops = {
            'a', 'à', 'ao', 'aos', 'aquela', 'aquelas', 'aquele', 'aqueles', 'aquilo',
            'as', 'até', 'com', 'como', 'da', 'das', 'de', 'dela', 'delas', 'dele',
            'deles', 'depois', 'do', 'dos', 'e', 'ela', 'elas', 'ele', 'eles', 'em',
            'entre', 'era', 'eram', 'essa', 'essas', 'esse', 'esses', 'esta', 'está',
            'estamos', 'estão', 'estas', 'estava', 'estavam', 'este', 'esteja', 'estes',
            'esteve', 'estive', 'estivemos', 'estiver', 'estivera', 'estiveram',
            'estiverem', 'estivermos', 'estivesse', 'estivessem', 'estou', 'eu', 'foi',
            'fomos', 'for', 'fora', 'foram', 'forem', 'formos', 'fosse', 'fossem',
            'fui', 'há', 'haja', 'hajam', 'hajamos', 'hão', 'havemos', 'haver',
            'haverá', 'haverão', 'haveria', 'haveriam', 'hei', 'houve', 'houvemos',
            'houver', 'houvera', 'houverá', 'houveram', 'houverei', 'houverem',
            'houveremos', 'houveria', 'houveriam', 'houvermos', 'houvesse', 'houvessem',
            'isso', 'isto', 'já', 'lhe', 'lhes', 'mais', 'mas', 'me', 'mesmo', 'meu',
            'meus', 'minha', 'minhas', 'muito', 'na', 'não', 'nas', 'nem', 'no', 'nos',
            'nós', 'nossa', 'nossas', 'nosso', 'nossos', 'num', 'numa', 'o', 'os',
            'ou', 'para', 'pela', 'pelas', 'pelo', 'pelos', 'por', 'que', 'quem',
            'se', 'seja', 'sejam', 'sejamos', 'sem', 'será', 'serão', 'seria', 'seriam',
            'seu', 'seus', 'só', 'somos', 'sou', 'sua', 'suas', 'também', 'te', 'tem',
            'tém', 'temos', 'tenha', 'tenham', 'tenhamos', 'tenho', 'ter', 'terá',
            'terão', 'teria', 'teriam', 'teu', 'teus', 'teve', 'tinha', 'tinham',
            'tive', 'tivemos', 'tiver', 'tivera', 'tiveram', 'tiverem', 'tivermos',
            'tivesse', 'tivessem', 'tu', 'tua', 'tuas', 'um', 'uma', 'você', 'vocês',
            'vos', 'à', 'às', 'é', 'são', 'quando', 'onde', 'ainda', 'então', 'depois',
            'antes', 'aqui', 'ali', 'bem', 'muito', 'pouco', 'todo', 'toda', 'todos',
            'todas', 'outro', 'outra', 'outros', 'outras', 'qual', 'quais', 'quanto',
            'quanta', 'quantos', 'quantas', 'algum', 'alguma', 'alguns', 'algumas',
            'nenhum', 'nenhuma', 'nenhuns', 'nenhumas'
        }

        # Tenta usar stopwords do NLTK se disponível
        if NLTK_AVAILABLE and stopwords_module:
            try:
                portuguese_stops = set(stopwords_module.words('portuguese'))
            except:
                portuguese_stops = basic_portuguese_stops
        else:
            portuguese_stops = basic_portuguese_stops

        # Adiciona stopwords específicas para redes sociais e contexto
        custom_stops = {
            'rt', 'via', 'http', 'https', 'www', 'com', 'br', 'org',
            'agronegocio', 'agronegócio', 'pra', 'pro', 'vc', 'vcs',
            'né', 'tá', 'tb', 'tbm', 'aí', 'né', 'rs', 'kkk', 'kk',
            'haha', 'rsrs', 'bluesky', 'post', 'twitter', 'x',
            'thread', 'repost', 'compartilhar', 'curtir', 'like',
            'hoje', 'ontem', 'amanhã', 'agora', 'ai', 'la', 'ta',
            'eh', 'eh', 'pq', 'q', 'eh', 'neh', 'tipo', 'cara',
            'mano', 'galera', 'pessoal', 'gente', 'ver', 'vou',
            'vai', 'fazer', 'ser', 'ter', 'estar', 'dar', 'ficar',
            'ir', 'vir', 'dizer', 'falar', 'saber', 'querer'
        }

        return portuguese_stops.union(custom_stops)

    def simple_tokenize(self, text: str) -> list:
        """
        Tokenização simples alternativa caso NLTK não funcione.

        Args:
            text (str): Texto para tokenizar

        Returns:
            list: Lista de tokens/palavras
        """
        # Remove pontuação e divide por espaços
        text = re.sub(r'[^\w\sáàâãéèêíìîóòôõúùûç]', ' ', text)
        words = text.split()
        return [word.strip() for word in words if word.strip()]

    def load_data(self) -> pd.DataFrame:
        """
        Carrega os dados da planilha Excel.

        Returns:
            pd.DataFrame: DataFrame com os dados carregados
        """
        try:
            self.df = pd.read_excel(self.excel_path)
            print(f"Dados carregados: {len(self.df)} posts encontrados")
            return self.df
        except Exception as e:
            print(f"Erro ao carregar planilha: {e}")
            return None

    def clean_text(self, text: str) -> str:
        """
        Limpa e preprocessa o texto dos posts.

        Args:
            text (str): Texto bruto do post

        Returns:
            str: Texto limpo e processado
        """
        if pd.isna(text):
            return ""

        # Converte para string e minúsculas
        text = str(text).lower()

        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

        # Remove menções (@usuario)
        text = re.sub(r'@\w+', '', text)

        # Remove hashtags mantendo o conteúdo
        text = re.sub(r'#(\w+)', r'\1', text)

        # Remove caracteres especiais, mantendo acentos
        text = re.sub(r'[^\w\sáàâãéèêíìîóòôõúùûç]', ' ', text)

        # Remove números isolados
        text = re.sub(r'\b\d+\b', '', text)

        # Remove espaços extras
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def process_all_text(self) -> str:
        """
        Processa todo o texto da coluna 'text' da planilha.

        Returns:
            str: Texto processado de todos os posts concatenados
        """
        if self.df is None:
            print("Erro: Dados não carregados. Execute load_data() primeiro.")
            return ""

        # Verifica se a coluna 'text' existe
        if 'text' not in self.df.columns:
            print("Erro: Coluna 'text' não encontrada na planilha.")
            print(f"Colunas disponíveis: {list(self.df.columns)}")
            return ""

        all_text = []

        for text in self.df['text'].dropna():
            cleaned = self.clean_text(text)
            if cleaned:
                all_text.append(cleaned)

        self.processed_text = " ".join(all_text)
        print(f"Texto processado: {len(self.processed_text)} caracteres")

        return self.processed_text

    def get_word_frequencies(self, min_word_length: int = 3) -> Counter:
        """
        Calcula a frequência das palavras no texto processado.

        Args:
            min_word_length (int): Comprimento mínimo das palavras

        Returns:
            Counter: Contador com frequências das palavras
        """
        if not self.processed_text:
            print("Erro: Texto não processado. Execute process_all_text() primeiro.")
            return Counter()

        # Tenta usar tokenização do NLTK, senão usa alternativa simples
        try:
            if NLTK_AVAILABLE and word_tokenize_func:
                tokens = word_tokenize_func(self.processed_text, language='portuguese')
            else:
                tokens = self.simple_tokenize(self.processed_text)
        except Exception as e:
            print(f"Erro na tokenização NLTK: {e}")
            print("Usando tokenização alternativa...")
            tokens = self.simple_tokenize(self.processed_text)

        # Filtra palavras
        filtered_words = [
            word for word in tokens 
            if (len(word) >= min_word_length and 
                word not in self.stop_words and 
                word.isalpha())
        ]

        return Counter(filtered_words)

    def generate_wordcloud(self, max_words: int = 100, 
                          figsize: tuple = (15, 8)) -> None:
        """
        Gera e exibe a nuvem de palavras.

        Args:
            max_words (int): Número máximo de palavras na nuvem
            figsize (tuple): Tamanho da figura (largura, altura)
        """
        word_freq = self.get_word_frequencies()

        if not word_freq:
            print("Erro: Nenhuma palavra encontrada para gerar a nuvem.")
            return

        # Configuração da nuvem de palavras
        wordcloud = WordCloud(
            width=1200,
            height=600,
            background_color='white',
            max_words=max_words,
            colormap='viridis',
            font_path=None,  # Use fonte padrão
            relative_scaling=0.5,
            min_font_size=10,
            max_font_size=100,
            prefer_horizontal=0.7
        ).generate_from_frequencies(word_freq)

        # Cria a visualização
        plt.figure(figsize=figsize)
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Nuvem de Palavras - Posts sobre Agronegócio no Bluesky', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.show()

    def show_top_words(self, n: int = 20) -> None:
        """
        Exibe as palavras mais frequentes em formato de tabela.

        Args:
            n (int): Número de palavras mais frequentes para exibir
        """
        word_freq = self.get_word_frequencies()

        if not word_freq:
            print("Erro: Nenhuma palavra encontrada.")
            return

        top_words = word_freq.most_common(n)

        print(f"\n=== TOP {n} PALAVRAS MAIS FREQUENTES ===")
        print(f"{'Palavra':<20} {'Frequência':<10}")
        print("-" * 30)

        for word, freq in top_words:
            print(f"{word:<20} {freq:<10}")

    def save_results(self, output_path: str = "wordcloud_agronegocio.png") -> None:
        """
        Salva a nuvem de palavras como arquivo PNG.

        Args:
            output_path (str): Caminho para salvar a imagem
        """
        word_freq = self.get_word_frequencies()

        if not word_freq:
            print("Erro: Nenhuma palavra encontrada para salvar.")
            return

        wordcloud = WordCloud(
            width=1200,
            height=600,
            background_color='white',
            max_words=100,
            colormap='viridis',
            relative_scaling=0.5,
            min_font_size=10,
            max_font_size=100,
            prefer_horizontal=0.7
        ).generate_from_frequencies(word_freq)

        wordcloud.to_file(output_path)
        print(f"Nuvem de palavras salva em: {output_path}")


def main():
    """Função principal para executar a análise."""
    # ALTERE O CAMINHO PARA SUA PLANILHA AQUI
    excel_path = "C:/Users/DavidosSantosVillela/Desktop/bluesky_agronegocio_30-07-2025.xlsx"

    # Cria o gerador
    generator = BlueskyWordCloudGenerator(excel_path)

    # Carrega os dados
    df = generator.load_data()

    if df is not None:
        # Processa o texto
        generator.process_all_text()

        # Mostra as palavras mais frequentes
        generator.show_top_words(30)

        # Gera a nuvem de palavras
        generator.generate_wordcloud()

        # Salva a imagem
        generator.save_results()


if __name__ == "__main__":
    main()

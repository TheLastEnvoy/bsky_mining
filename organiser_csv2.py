import pandas as pd
from datetime import datetime
import os

class BlueskyPostFormatter:
    def __init__(self):
        self.posts = []
        self.sentiment_column = None

    def detect_sentiment_column(self, df):
        """Detecta automaticamente qual coluna contém os sentimentos"""
        possible_columns = [
            'sentiment_agronegocio', 
            'sentiment', 
            'sentimento', 
            'classificacao',
            'analise_sentimento'
        ]

        for col in possible_columns:
            if col in df.columns:
                print(f"✅ Coluna de sentimento encontrada: '{col}'")
                return col

        # Se não encontrar, verifica se há alguma coluna com valores típicos de sentimento
        for col in df.columns:
            if df[col].dtype == 'object':  # Apenas colunas de texto
                unique_values = df[col].dropna().str.lower().unique()
                sentiment_words = {'positivo', 'negativo', 'neutro', 'positive', 'negative', 'neutral'}
                if len(set(unique_values) & sentiment_words) >= 2:
                    print(f"✅ Possível coluna de sentimento detectada: '{col}'")
                    return col

        print("⚠️ Coluna de sentimento não encontrada automaticamente")
        return None

    def load_csv(self, input_file: str):
        """Carrega o CSV original"""
        try:
            # Tenta diferentes encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'utf-8-sig']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(input_file, encoding=encoding)
                    print(f"✅ Arquivo carregado com encoding: {encoding}")
                    break
                except Exception as e:
                    continue

            if df is None:
                print("❌ Erro ao carregar o arquivo com nenhum encoding testado")
                return False

            print(f"📊 Total de registros: {len(df)}")
            print(f"📋 Colunas encontradas: {list(df.columns)}")

            # Detecta coluna de sentimento
            self.sentiment_column = self.detect_sentiment_column(df)
            if not self.sentiment_column:
                print("❌ Não foi possível detectar a coluna de sentimento")
                print("📋 Colunas disponíveis:", list(df.columns))

                # Pergunta ao usuário qual coluna usar
                sentiment_col = input("Digite o nome da coluna que contém os sentimentos: ").strip()
                if sentiment_col in df.columns:
                    self.sentiment_column = sentiment_col
                    print(f"✅ Usando coluna: '{sentiment_col}'")
                else:
                    print(f"❌ Coluna '{sentiment_col}' não encontrada")
                    return False

            # Mostra distribuição dos sentimentos
            sentiment_dist = df[self.sentiment_column].value_counts()
            print(f"\n📊 Distribuição dos sentimentos na coluna '{self.sentiment_column}':")
            for sent, count in sentiment_dist.items():
                print(f"   {sent}: {count}")

            # Processa cada linha
            self.posts = []
            for _, row in df.iterrows():
                post = {
                    'author_handle': str(row.get('author_handle', '')).strip(),
                    'author_display_name': str(row.get('author_display_name', '')).strip(),
                    'created_at': str(row.get('created_at', '')).strip(),
                    'sentiment': str(row.get(self.sentiment_column, 'neutro')).strip().lower(),
                    'like_count': self._safe_int(row.get('like_count', 0)),
                    'repost_count': self._safe_int(row.get('repost_count', 0)),
                    'reply_count': self._safe_int(row.get('reply_count', 0)),
                    'text': str(row.get('text', '')).strip(),
                    'web_link': str(row.get('web_link', '')).strip(),
                    'uri': str(row.get('uri', '')).strip(),
                    # Preserva colunas extras se existirem
                    'contexto_agronegocio': str(row.get('contexto_agronegocio', '')).strip() if 'contexto_agronegocio' in df.columns else '',
                    'cid': str(row.get('cid', '')).strip() if 'cid' in df.columns else '',
                    'author_did': str(row.get('author_did', '')).strip() if 'author_did' in df.columns else ''
                }

                # Só adiciona se tiver dados válidos
                if post['author_handle'] and post['text']:
                    self.posts.append(post)

            print(f"✅ Posts válidos: {len(self.posts)}")

            # Verifica se os sentimentos foram carregados corretamente
            loaded_sentiments = {}
            for post in self.posts:
                sent = post['sentiment']
                loaded_sentiments[sent] = loaded_sentiments.get(sent, 0) + 1

            print(f"\n✅ Sentimentos carregados:")
            for sent, count in loaded_sentiments.items():
                print(f"   {sent}: {count}")

            return True

        except Exception as e:
            print(f"❌ Erro: {e}")
            return False

    def _safe_int(self, value):
        """Converte valor para int de forma segura"""
        try:
            return int(float(str(value))) if str(value) not in ['', 'nan', 'None'] else 0
        except:
            return 0

    def format_date(self, date_str: str) -> str:
        """Formata data para leitura brasileira"""
        try:
            if not date_str or date_str == 'nan':
                return "Data não disponível"

            clean_date = date_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(clean_date)
            return dt.strftime("%d/%m/%Y às %H:%M")
        except:
            return date_str

    def generate_web_link(self, uri: str, handle: str) -> str:
        """Gera link web se não existir"""
        try:
            if not uri or not handle:
                return "Link não disponível"
            post_id = uri.split('/')[-1] if '/' in uri else ""
            if post_id:
                return f"https://bsky.app/profile/{handle}/post/{post_id}"
            return "Link não disponível"
        except:
            return "Link não disponível"

    def save_organized_format(self, output_file: str = None):
        """Salva no formato organizado solicitado"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"bluesky_posts_organizados_{timestamp}.txt"

        if not self.posts:
            print("❌ Nenhum post para salvar")
            return

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("POSTS SOBRE AGRONEGÓCIO - BLUESKY 2025\n")
            f.write("=" * 50 + "\n\n")

            for i, post in enumerate(self.posts, 1):
                # Garante que o web_link existe
                web_link = post['web_link']
                if not web_link or web_link == 'nan':
                    web_link = self.generate_web_link(post['uri'], post['author_handle'])

                f.write(f"POST #{i}\n")
                f.write("-" * 30 + "\n")
                f.write(f"author_handle: {post['author_handle']}\n")
                f.write(f"author_display_name: {post['author_display_name']}\n")
                f.write(f"created_at: {self.format_date(post['created_at'])}\n")
                f.write(f"sentiment: {post['sentiment']}\n")
                f.write(f"like_count: {post['like_count']}\n")
                f.write(f"repost_count: {post['repost_count']}\n")
                f.write(f"reply_count: {post['reply_count']}\n")
                f.write(f"text: {post['text']}\n")
                f.write(f"web_link: {web_link}\n")
                f.write(f"uri: {post['uri']}\n")

                # Adiciona campos extras se existirem
                if post.get('contexto_agronegocio'):
                    f.write(f"contexto_agronegocio: {post['contexto_agronegocio']}\n")
                if post.get('cid'):
                    f.write(f"cid: {post['cid']}\n")
                if post.get('author_did'):
                    f.write(f"author_did: {post['author_did']}\n")

                f.write("\n" + "=" * 50 + "\n\n")

        print(f"💾 Arquivo organizado salvo em: {output_file}")

    def save_csv_clean(self, output_file: str = None):
        """Salva CSV limpo e organizado"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"bluesky_posts_limpo_{timestamp}.csv"

        if not self.posts:
            print("❌ Nenhum post para salvar")
            return

        # Cria DataFrame organizado
        df_clean = pd.DataFrame(self.posts)

        # Define ordem das colunas (principais primeiro)
        main_columns = [
            'author_handle', 'author_display_name', 'created_at', 
            'sentiment', 'like_count', 'repost_count', 'reply_count',
            'text', 'web_link', 'uri'
        ]

        # Adiciona colunas extras se existirem
        extra_columns = [col for col in df_clean.columns if col not in main_columns]
        column_order = main_columns + extra_columns

        # Filtra apenas colunas que existem
        column_order = [col for col in column_order if col in df_clean.columns]
        df_clean = df_clean[column_order]

        # Salva CSV limpo
        df_clean.to_csv(output_file, index=False, encoding='utf-8')
        print(f"💾 CSV limpo salvo em: {output_file}")

    def save_excel_organized(self, output_file: str = None):
        """Salva Excel com formatação melhorada"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"bluesky_posts_excel_{timestamp}.xlsx"

        if not self.posts:
            print("❌ Nenhum post para salvar")
            return

        try:
            df = pd.DataFrame(self.posts)

            # Define ordem das colunas
            main_columns = [
                'author_handle', 'author_display_name', 'created_at', 
                'sentiment', 'like_count', 'repost_count', 'reply_count',
                'text', 'web_link', 'uri'
            ]

            extra_columns = [col for col in df.columns if col not in main_columns]
            column_order = main_columns + extra_columns
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]

            # Formata datas
            df['created_at_formatted'] = df['created_at'].apply(self.format_date)

            # Salva Excel
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Posts Agronegócio', index=False)

                # Ajusta largura das colunas
                worksheet = writer.sheets['Posts Agronegócio']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            print(f"📊 Excel organizado salvo em: {output_file}")

        except ImportError:
            print("⚠️ Para salvar Excel, instale: pip install openpyxl")
        except Exception as e:
            print(f"❌ Erro ao salvar Excel: {e}")

    def save_sentiment_separated(self, base_filename: str = None):
        """Salva arquivos separados por sentimento"""
        if not base_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"bluesky_posts_{timestamp}"

        if not self.posts:
            print("❌ Nenhum post para salvar")
            return

        # Agrupa por sentimento
        sentiments_groups = {}
        for post in self.posts:
            sentiment = post['sentiment']
            if sentiment not in sentiments_groups:
                sentiments_groups[sentiment] = []
            sentiments_groups[sentiment].append(post)

        # Salva arquivo para cada sentimento
        for sentiment, posts in sentiments_groups.items():
            filename = f"{base_filename}_{sentiment}.csv"
            df = pd.DataFrame(posts)

            main_columns = [
                'author_handle', 'author_display_name', 'created_at', 
                'sentiment', 'like_count', 'repost_count', 'reply_count',
                'text', 'web_link', 'uri'
            ]

            extra_columns = [col for col in df.columns if col not in main_columns]
            column_order = main_columns + extra_columns
            column_order = [col for col in column_order if col in df.columns]
            df = df[column_order]

            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"💾 Posts {sentiment}: {filename} ({len(posts)} posts)")

    def print_sample(self, num_posts: int = 3):
        """Mostra uma amostra no formato organizado"""
        print(f"\n📋 AMOSTRA DOS PRIMEIROS {num_posts} POSTS:")
        print("=" * 60)

        for i, post in enumerate(self.posts[:num_posts], 1):
            web_link = post['web_link']
            if not web_link or web_link == 'nan':
                web_link = self.generate_web_link(post['uri'], post['author_handle'])

            print(f"\nPOST #{i}")
            print("-" * 30)
            print(f"author_handle: {post['author_handle']}")
            print(f"author_display_name: {post['author_display_name']}")
            print(f"created_at: {self.format_date(post['created_at'])}")
            print(f"sentiment: {post['sentiment']}")
            print(f"like_count: {post['like_count']}")
            print(f"repost_count: {post['repost_count']}")
            print(f"reply_count: {post['reply_count']}")
            print(f"text: {post['text']}")
            print(f"web_link: {web_link}")
            print(f"uri: {post['uri']}")
            if post.get('contexto_agronegocio'):
                print(f"contexto_agronegocio: {post['contexto_agronegocio']}")
            print("=" * 60)

    def generate_statistics(self):
        """Gera estatísticas dos posts"""
        if not self.posts:
            return

        total = len(self.posts)
        total_likes = sum(p['like_count'] for p in self.posts)
        total_reposts = sum(p['repost_count'] for p in self.posts)
        total_replies = sum(p['reply_count'] for p in self.posts)

        print(f"\n📊 ESTATÍSTICAS GERAIS:")
        print(f"📝 Total de posts: {total:,}")
        print(f"💖 Total de likes: {total_likes:,}")
        print(f"🔄 Total de reposts: {total_reposts:,}")
        print(f"💬 Total de replies: {total_replies:,}")

        if total > 0:
            print(f"📈 Média de likes por post: {total_likes/total:.1f}")
            print(f"📈 Média de reposts por post: {total_reposts/total:.1f}")
            print(f"📈 Média de replies por post: {total_replies/total:.1f}")

        # Sentimentos (já classificados)
        sentiments = {}
        for post in self.posts:
            sent = post['sentiment']
            sentiments[sent] = sentiments.get(sent, 0) + 1

        print(f"\n🎭 SENTIMENTOS JÁ CLASSIFICADOS (coluna: {self.sentiment_column}):")
        for sentiment, count in sorted(sentiments.items()):
            percentage = (count/total)*100
            emoji = {'positivo': '😊', 'negativo': '😞', 'neutro': '😐'}.get(sentiment, '❓')
            print(f"   {emoji} {sentiment.capitalize()}: {count:,} posts ({percentage:.1f}%)")

        # Top posts por engajamento
        print(f"\n🏆 TOP 5 POSTS POR ENGAJAMENTO:")
        sorted_posts = sorted(self.posts, 
                            key=lambda x: x['like_count'] + x['repost_count'] + x['reply_count'], 
                            reverse=True)

        for i, post in enumerate(sorted_posts[:5], 1):
            total_engagement = post['like_count'] + post['repost_count'] + post['reply_count']
            sentiment_emoji = {'positivo': '😊', 'negativo': '😞', 'neutro': '😐'}.get(post['sentiment'], '❓')
            print(f"\n{i}. @{post['author_handle']} {sentiment_emoji}")
            print(f"   Engajamento: {total_engagement:,} (👍{post['like_count']} 🔄{post['repost_count']} 💬{post['reply_count']})")
            print(f"   Data: {self.format_date(post['created_at'])}")
            print(f"   Texto: {post['text'][:120]}{'...' if len(post['text']) > 120 else ''}")

        # Estatísticas por sentimento
        print(f"\n📈 ENGAJAMENTO POR SENTIMENTO:")
        for sentiment in sorted(sentiments.keys()):
            posts_sentiment = [p for p in self.posts if p['sentiment'] == sentiment]
            if posts_sentiment:
                avg_likes = sum(p['like_count'] for p in posts_sentiment) / len(posts_sentiment)
                avg_reposts = sum(p['repost_count'] for p in posts_sentiment) / len(posts_sentiment)
                avg_replies = sum(p['reply_count'] for p in posts_sentiment) / len(posts_sentiment)
                emoji = {'positivo': '😊', 'negativo': '😞', 'neutro': '😐'}.get(sentiment, '❓')
                print(f"   {emoji} {sentiment.capitalize()}:")
                print(f"      Média likes: {avg_likes:.1f} | reposts: {avg_reposts:.1f} | replies: {avg_replies:.1f}")

def main():
    """Função principal"""
    formatter = BlueskyPostFormatter()

    print("🚀 === ORGANIZADOR DE POSTS BLUESKY ===")
    print("Transforma CSV confuso em formato organizado e legível\n")

    # Solicita arquivo
    input_file = input("📁 Digite o nome do arquivo CSV: ").strip()

    if not input_file:
        print("❌ Nome do arquivo não fornecido")
        return

    if not os.path.exists(input_file):
        print(f"❌ Arquivo '{input_file}' não encontrado")
        return

    # Carrega dados
    if not formatter.load_csv(input_file):
        return

    # Mostra opções
    print(f"\n📋 O que você deseja fazer?")
    print("1 - Ver amostra no formato organizado")
    print("2 - Salvar arquivo TXT organizado (formato solicitado)")
    print("3 - Salvar CSV limpo")
    print("4 - Salvar Excel organizado")
    print("5 - Salvar CSVs separados por sentimento")
    print("6 - Ver estatísticas detalhadas")
    print("7 - Fazer tudo")

    choice = input("\nEscolha uma opção (1-7): ").strip()

    if choice in ['1', '7']:
        formatter.print_sample(5)

    if choice in ['2', '7']:
        formatter.save_organized_format()

    if choice in ['3', '7']:
        formatter.save_csv_clean()

    if choice in ['4', '7']:
        formatter.save_excel_organized()

    if choice in ['5', '7']:
        formatter.save_sentiment_separated()

    if choice in ['6', '7']:
        formatter.generate_statistics()  # CORREÇÃO AQUI

    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()

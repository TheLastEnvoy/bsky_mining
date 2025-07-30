import pandas as pd
import csv
from datetime import datetime
import json

class BlueskyCSVOrganizer:
    def __init__(self):
        self.posts = []

    def load_and_clean_csv(self, input_file: str):
        """Carrega e limpa o CSV"""
        try:
            # Tenta diferentes encodings
            encodings = ['utf-8', 'latin-1', 'cp1252']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(input_file, encoding=encoding)
                    print(f"✅ Arquivo carregado com encoding: {encoding}")
                    break
                except:
                    continue

            if df is None:
                print("❌ Erro ao carregar o arquivo")
                return False

            print(f"📊 Total de posts no arquivo: {len(df)}")

            # Limpa e organiza os dados
            self.posts = []
            for _, row in df.iterrows():
                post_data = {
                    'uri': str(row.get('uri', '')),
                    'author_handle': str(row.get('author_handle', '')),
                    'author_display_name': str(row.get('author_display_name', '')),
                    'text': str(row.get('text', '')),
                    'created_at': str(row.get('created_at', '')),
                    'reply_count': int(row.get('reply_count', 0)),
                    'repost_count': int(row.get('repost_count', 0)),
                    'like_count': int(row.get('like_count', 0)),
                    'sentiment': str(row.get('sentiment', 'neutro')),
                    'web_link': self.convert_uri_to_web_url(
                        str(row.get('uri', '')), 
                        str(row.get('author_handle', ''))
                    )
                }

                # Só adiciona se tiver dados válidos
                if post_data['text'] and post_data['author_handle']:
                    self.posts.append(post_data)

            print(f"✅ Posts válidos organizados: {len(self.posts)}")
            return True

        except Exception as e:
            print(f"❌ Erro ao processar arquivo: {e}")
            return False

    def convert_uri_to_web_url(self, uri: str, handle: str) -> str:
        """Converte URI para link web"""
        try:
            if not uri or not handle:
                return ""
            post_id = uri.split('/')[-1] if '/' in uri else ""
            if post_id:
                return f"https://bsky.app/profile/{handle}/post/{post_id}"
            return ""
        except:
            return ""

    def format_date(self, date_str: str) -> str:
        """Formata data para leitura"""
        try:
            if not date_str or date_str == 'nan':
                return "Data não disponível"

            # Remove 'Z' e converte
            clean_date = date_str.replace('Z', '+00:00')
            dt = datetime.fromisoformat(clean_date)
            return dt.strftime("%d/%m/%Y às %H:%M")
        except:
            return date_str

    def print_organized_posts(self):
        """Imprime posts organizados no console"""
        print("\n" + "="*80)
        print("📋 POSTS ORGANIZADOS - AGRONEGÓCIO NO BLUESKY")
        print("="*80)

        for i, post in enumerate(self.posts, 1):
            print(f"\n📝 POST #{i}")
            print("-" * 60)
            print(f"👤 AUTOR: @{post['author_handle']}")
            print(f"🏷️  NOME: {post['author_display_name']}")
            print(f"📅 DATA: {self.format_date(post['created_at'])}")
            print(f"🎭 SENTIMENTO: {post['sentiment'].upper()}")
            print(f"\n💬 TEXTO:")
            print(f"   {post['text']}")
            print(f"\n📊 ENGAJAMENTO:")
            print(f"   💖 Likes: {post['like_count']}")
            print(f"   🔄 Reposts: {post['repost_count']}")
            print(f"   💬 Replies: {post['reply_count']}")
            print(f"\n🔗 LINK: {post['web_link']}")

            if i < len(self.posts):
                print("\n" + "─" * 80)

    def save_organized_csv(self, output_file: str = None):
        """Salva CSV organizado"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"bluesky_posts_organizados_{timestamp}.csv"

        if not self.posts:
            print("❌ Nenhum post para salvar")
            return

        # Define as colunas na ordem desejada
        fieldnames = [
            'author_handle', 'author_display_name', 'created_at', 
            'sentiment', 'like_count', 'repost_count', 'reply_count',
            'text', 'web_link', 'uri'
        ]

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for post in self.posts:
                # Reorganiza na ordem correta
                row = {field: post.get(field, '') for field in fieldnames}
                writer.writerow(row)

        print(f"💾 CSV organizado salvo em: {output_file}")

    def save_readable_txt(self, output_file: str = None):
        """Salva arquivo TXT legível"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"bluesky_posts_legivel_{timestamp}.txt"

        with open(output_file, 'w', encoding='utf-8') as txtfile:
            txtfile.write("POSTS SOBRE AGRONEGÓCIO NO BLUESKY - 2025\n")
            txtfile.write("="*60 + "\n\n")

            for i, post in enumerate(self.posts, 1):
                txtfile.write(f"POST #{i}\n")
                txtfile.write("-" * 40 + "\n")
                txtfile.write(f"Autor: @{post['author_handle']}\n")
                txtfile.write(f"Nome: {post['author_display_name']}\n")
                txtfile.write(f"Data: {self.format_date(post['created_at'])}\n")
                txtfile.write(f"Sentimento: {post['sentiment'].upper()}\n")
                txtfile.write(f"Engajamento: {post['like_count']} likes, {post['repost_count']} reposts, {post['reply_count']} replies\n")
                txtfile.write(f"\nTexto:\n{post['text']}\n")
                txtfile.write(f"\nLink: {post['web_link']}\n")
                txtfile.write("\n" + "="*60 + "\n\n")

        print(f"📄 Arquivo legível salvo em: {output_file}")

    def generate_summary(self):
        """Gera resumo estatístico"""
        if not self.posts:
            return

        total_posts = len(self.posts)
        total_likes = sum(post['like_count'] for post in self.posts)
        total_reposts = sum(post['repost_count'] for post in self.posts)
        total_replies = sum(post['reply_count'] for post in self.posts)

        # Sentimentos
        sentiments = {}
        for post in self.posts:
            sent = post['sentiment']
            sentiments[sent] = sentiments.get(sent, 0) + 1

        # Autores mais ativos
        authors = {}
        for post in self.posts:
            author = post['author_handle']
            authors[author] = authors.get(author, 0) + 1

        top_authors = sorted(authors.items(), key=lambda x: x[1], reverse=True)[:10]

        print(f"\n📊 === RESUMO ESTATÍSTICO ===")
        print(f"📝 Total de posts: {total_posts}")
        print(f"💖 Total de likes: {total_likes}")
        print(f"🔄 Total de reposts: {total_reposts}")
        print(f"💬 Total de replies: {total_replies}")

        if total_posts > 0:
            print(f"📈 Média de likes por post: {total_likes/total_posts:.1f}")
            print(f"📈 Média de reposts por post: {total_reposts/total_posts:.1f}")

        print(f"\n🎭 ANÁLISE DE SENTIMENTO:")
        for sentiment, count in sentiments.items():
            percentage = (count/total_posts)*100
            print(f"   {sentiment.capitalize()}: {count} posts ({percentage:.1f}%)")

        print(f"\n👥 TOP 10 AUTORES MAIS ATIVOS:")
        for i, (author, count) in enumerate(top_authors, 1):
            print(f"   {i}. @{author}: {count} posts")

def main():
    """Função principal"""
    organizer = BlueskyCSVOrganizer()

    print("🚀 === ORGANIZADOR DE POSTS DO BLUESKY ===")
    print("Este script vai organizar e limpar seu CSV de posts\n")

    # Solicita o arquivo
    input_file = input("📁 Digite o nome do arquivo CSV: ").strip()

    if not input_file:
        print("❌ Nome do arquivo não fornecido")
        return

    # Carrega e processa
    if not organizer.load_and_clean_csv(input_file):
        return

    # Mostra opções
    print(f"\n📋 O que você deseja fazer?")
    print("1 - Ver posts organizados no console")
    print("2 - Salvar CSV organizado")
    print("3 - Salvar arquivo TXT legível")
    print("4 - Ver resumo estatístico")
    print("5 - Fazer tudo")

    choice = input("\nEscolha uma opção (1-5): ").strip()

    if choice in ['1', '5']:
        organizer.print_organized_posts()

    if choice in ['2', '5']:
        organizer.save_organized_csv()

    if choice in ['3', '5']:
        organizer.save_readable_txt()

    if choice in ['4', '5']:
        organizer.generate_summary()

    print(f"\n✅ Processamento concluído!")

if __name__ == "__main__":
    main()

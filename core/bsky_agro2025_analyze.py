import requests
import json
import time
from datetime import datetime, timezone
import csv
from typing import List, Dict, Optional
import os
from getpass import getpass

class BlueskySearcher2025:
    def __init__(self):
        self.base_url = "https://bsky.social"
        self.session = requests.Session()
        self.access_token = None
        self.session.headers.update({
            'User-Agent': 'BlueskyAgroSearcher2025/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        # Filtros de data para 2025
        self.start_date = "2025-01-01T00:00:00Z"
        self.end_date = "2025-12-31T23:59:59Z"

    def create_session(self, identifier: str, password: str) -> bool:
        """Cria sessão autenticada no Bluesky"""
        endpoint = f"{self.base_url}/xrpc/com.atproto.server.createSession"

        data = {
            "identifier": identifier,
            "password": password
        }

        try:
            response = self.session.post(endpoint, json=data)
            response.raise_for_status()

            session_data = response.json()
            self.access_token = session_data.get('accessJwt')

            if self.access_token:
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}'
                })
                print("✅ Autenticação realizada com sucesso!")
                return True
            else:
                print("❌ Erro: Token de acesso não encontrado")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na autenticação: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Status Code: {e.response.status_code}")
                print(f"Response: {e.response.text}")
            return False

    def search_posts_2025(self, query: str, limit: int = 25, cursor: Optional[str] = None) -> Dict:
        """Busca posts de 2025 usando a API autenticada do Bluesky"""
        if not self.access_token:
            print("❌ Erro: Não autenticado")
            return {}

        endpoint = f"{self.base_url}/xrpc/app.bsky.feed.searchPosts"

        params = {
            'q': query,
            'limit': min(limit, 25),
            'since': self.start_date,
            'until': self.end_date
        }

        if cursor:
            params['cursor'] = cursor

        try:
            response = self.session.get(endpoint, params=params)

            if response.status_code == 429:
                wait_time = 60
                print(f"⏳ Rate limit atingido. Aguardando {wait_time}s...")
                time.sleep(wait_time)
                return self.search_posts_2025(query, limit, cursor)

            if response.status_code == 403:
                print("❌ Acesso negado. Verifique suas credenciais.")
                return {}

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
            return {}

    def is_post_from_2025(self, post_date: str) -> bool:
        """Verifica se o post é de 2025"""
        try:
            if not post_date:
                return False

            # Converte a data do post para datetime
            post_datetime = datetime.fromisoformat(post_date.replace('Z', '+00:00'))

            # Verifica se é de 2025
            return post_datetime.year == 2025
        except Exception as e:
            print(f"⚠️ Erro ao verificar data: {e}")
            return False

    def extract_post_data(self, post: Dict) -> Dict:
        """Extrai dados relevantes de um post"""
        try:
            if 'post' in post:
                actual_post = post['post']
            else:
                actual_post = post

            record = actual_post.get('record', {})
            author = actual_post.get('author', {})
            created_at = record.get('createdAt', '')

            return {
                'uri': actual_post.get('uri', ''),
                'cid': actual_post.get('cid', ''),
                'author_did': author.get('did', ''),
                'author_handle': author.get('handle', ''),
                'author_display_name': author.get('displayName', ''),
                'text': record.get('text', ''),
                'created_at': created_at,
                'reply_count': actual_post.get('replyCount', 0),
                'repost_count': actual_post.get('repostCount', 0),
                'like_count': actual_post.get('likeCount', 0),
                'indexed_at': actual_post.get('indexedAt', ''),
                'langs': record.get('langs', []),
                'is_2025': self.is_post_from_2025(created_at)
            }
        except Exception as e:
            print(f"❌ Erro ao extrair dados: {e}")
            return {}

    def filter_posts_by_keyword_and_year(self, posts: List[Dict], keyword: str) -> List[Dict]:
        """Filtra posts de 2025 que contêm a palavra-chave"""
        filtered_posts = []
        keyword_lower = keyword.lower()

        for post in posts:
            text = post.get('text', '').lower()
            created_at = post.get('created_at', '')

            # Verifica se contém a palavra-chave E é de 2025
            if keyword_lower in text and self.is_post_from_2025(created_at):
                filtered_posts.append(post)

        return filtered_posts

    def collect_all_posts_2025(self, query: str, delay: float = 3.0, max_requests: int = 10000) -> List[Dict]:
        """Coleta TODOS os posts de 2025 (sem limite de quantidade)"""
        all_posts = []
        cursor = None
        requests_made = 0
        posts_2025_found = 0

        print(f"🔍 Iniciando coleta COMPLETA de posts de 2025 com '{query}'...")
        print(f"📅 Período: 01/01/2025 a 31/12/2025")
        print(f"⏱️ Delay entre requests: {delay}s")
        print(f"🔄 Máximo de requests: {max_requests}")
        print("-" * 60)

        start_time = datetime.now()

        while requests_made < max_requests:
            requests_made += 1

            print(f"📡 Request {requests_made} - Posts de 2025 coletados: {posts_2025_found}")

            # Faz a busca com filtros de 2025
            result = self.search_posts_2025(query, 25, cursor)

            if not result or 'posts' not in result:
                print("❌ Sem resultados ou erro na API")
                break

            posts_raw = result.get('posts', [])

            if not posts_raw:
                print("📭 Não há mais posts disponíveis")
                break

            # Processa os posts
            batch_posts = []
            batch_2025_count = 0

            for post_raw in posts_raw:
                post_data = self.extract_post_data(post_raw)
                if post_data and post_data.get('text'):
                    batch_posts.append(post_data)

                    # Conta posts de 2025
                    if post_data.get('is_2025', False):
                        batch_2025_count += 1

            # Filtra apenas posts de 2025 com a palavra-chave
            filtered_posts = self.filter_posts_by_keyword_and_year(batch_posts, query)
            all_posts.extend(filtered_posts)
            posts_2025_found += len(filtered_posts)

            print(f"   📊 Posts neste lote: {len(posts_raw)}")
            print(f"   📅 Posts de 2025 neste lote: {batch_2025_count}")
            print(f"   🎯 Posts de 2025 com '{query}': {len(filtered_posts)}")

            # Atualiza cursor
            cursor = result.get('cursor')
            if not cursor:
                print("📄 Não há mais páginas disponíveis")
                break

            # Mostra progresso a cada 10 requests
            if requests_made % 10 == 0:
                elapsed = datetime.now() - start_time
                print(f"\n⏰ Progresso após {requests_made} requests:")
                print(f"   🕐 Tempo decorrido: {elapsed}")
                print(f"   📝 Total de posts de 2025: {posts_2025_found}")
                print(f"   📈 Taxa: {posts_2025_found / requests_made:.1f} posts/request")
                print("-" * 40)

            # Rate limiting
            if delay > 0:
                time.sleep(delay)

        elapsed = datetime.now() - start_time
        print(f"\n✅ === COLETA FINALIZADA ===")
        print(f"🕐 Tempo total: {elapsed}")
        print(f"📡 Total de requests: {requests_made}")
        print(f"📝 Posts de 2025 coletados: {len(all_posts)}")
        print(f"📈 Taxa final: {len(all_posts) / requests_made:.1f} posts/request")

        return all_posts

    def analyze_posts(self, posts: List[Dict]):
        """Análise estatística dos posts coletados"""
        if not posts:
            return

        print(f"\n📊 === ANÁLISE ESTATÍSTICA ===")

        # Estatísticas básicas
        total_likes = sum(post.get('like_count', 0) for post in posts)
        total_reposts = sum(post.get('repost_count', 0) for post in posts)
        total_replies = sum(post.get('reply_count', 0) for post in posts)

        print(f"📝 Total de posts: {len(posts)}")
        print(f"💖 Total de likes: {total_likes}")
        print(f"🔄 Total de reposts: {total_reposts}")
        print(f"💬 Total de replies: {total_replies}")

        if len(posts) > 0:
            print(f"📈 Média de likes/post: {total_likes / len(posts):.1f}")
            print(f"📈 Média de reposts/post: {total_reposts / len(posts):.1f}")

        # Posts por mês
        monthly_count = {}
        for post in posts:
            created_at = post.get('created_at', '')
            if created_at:
                try:
                    post_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                    month_key = f"{post_date.year}-{post_date.month:02d}"
                    monthly_count[month_key] = monthly_count.get(month_key, 0) + 1
                except:
                    pass

        if monthly_count:
            print(f"\n📅 === POSTS POR MÊS (2025) ===")
            for month, count in sorted(monthly_count.items()):
                print(f"   {month}: {count} posts")

        # Autores mais ativos
        author_count = {}
        for post in posts:
            handle = post.get('author_handle', '')
            if handle:
                author_count[handle] = author_count.get(handle, 0) + 1

        if author_count:
            print(f"\n👥 === TOP 10 AUTORES MAIS ATIVOS ===")
            top_authors = sorted(author_count.items(), key=lambda x: x[1], reverse=True)[:10]
            for i, (author, count) in enumerate(top_authors, 1):
                print(f"   {i}. @{author}: {count} posts")

    def save_to_csv(self, posts: List[Dict], filename: str = None):
        """Salva posts em arquivo CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/bluesky_agro_2025_complete_{timestamp}.csv"

        if not posts:
            print("❌ Nenhum post para salvar")
            return

        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = posts[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for post in posts:
                writer.writerow(post)

        print(f"💾 Posts salvos em: {filename}")

    def save_to_json(self, posts: List[Dict], filename: str = None):
        """Salva posts em arquivo JSON"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data/bluesky_agro_2025_complete_{timestamp}.json"

        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(posts, jsonfile, ensure_ascii=False, indent=2)

        print(f"💾 Posts salvos em: {filename}")

def load_env_file():
    """Carrega arquivo .env manualmente"""
    env_path = '.env'

    if os.path.exists(env_path):
        print("📄 Arquivo .env encontrado!")
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        value = value.strip('"\'')
                        os.environ[key.strip()] = value
            print("✅ Variáveis do .env carregadas!")
        except Exception as e:
            print(f"❌ Erro ao ler .env: {e}")
    else:
        print("📄 Arquivo .env não encontrado")

def get_credentials():
    """Obtém credenciais"""
    load_env_file()

    env_email = os.getenv('BLUESKY_EMAIL')
    env_password = os.getenv('BLUESKY_PASSWORD')

    if env_email and env_password:
        print("✅ Usando credenciais do arquivo .env")
        return env_email, env_password
    else:
        print("❌ Credenciais não encontradas no .env. Por favor, crie um arquivo .env com as variáveis BLUESKY_EMAIL e BLUESKY_PASSWORD.")
        return None, None

def main():
    """Função principal"""
    searcher = BlueskySearcher2025()

    # Configurações para busca completa de 2025
    QUERY = "agronegócio"
    DELAY_BETWEEN_REQUESTS = 3.0  # Mais conservador para coleta longa
    MAX_REQUESTS = 50000  # Limite alto para busca completa

    print("🚀 === COLETOR COMPLETO - POSTS 2025 ===")
    print(f"🔍 Buscando por: '{QUERY}'")
    print(f"📅 Período: Todo o ano de 2025")
    print(f"⏱️ Delay entre requests: {DELAY_BETWEEN_REQUESTS}s")
    print(f"🔄 Máximo de requests: {MAX_REQUESTS}")
    print(f"⚠️  ATENÇÃO: Esta pode ser uma coleta MUITO longa!")
    print("-" * 60)

    # Confirma se o usuário quer continuar
    confirm = input("🤔 Deseja continuar? (s/N): ").strip().lower()
    if confirm not in ['s', 'sim', 'y', 'yes']:
        print("❌ Operação cancelada pelo usuário")
        return

    # Obtém credenciais
    email, password = get_credentials()

    if not email or not password:
        print("❌ Credenciais não fornecidas. O script será encerrado.")
        return

    # Autentica
    if not searcher.create_session(email, password):
        print("❌ Falha na autenticação")
        return

    try:
        print(f"\n🏁 Iniciando coleta em 3 segundos...")
        time.sleep(3)

        # Coleta TODOS os posts de 2025
        posts = searcher.collect_all_posts_2025(
            query=QUERY,
            delay=DELAY_BETWEEN_REQUESTS,
            max_requests=MAX_REQUESTS
        )

        if posts:
            # Análise estatística
            searcher.analyze_posts(posts)

            # Salva os arquivos
            print(f"\n💾 === SALVANDO ARQUIVOS ===")
            searcher.save_to_csv(posts)
            searcher.save_to_json(posts)

            print(f"\n🎉 SUCESSO! Coletados {len(posts)} posts de 2025 com 'agronegócio'!")

        else:
            print("❌ Nenhum post foi coletado.")

    except KeyboardInterrupt:
        print("\n\n⏹️ Interrompido pelo usuário.")
        print("💾 Salvando posts coletados até agora...")
        if 'posts' in locals() and posts:
            searcher.save_to_csv(posts)
            searcher.save_to_json(posts)
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")

if __name__ == "__main__":
    main()

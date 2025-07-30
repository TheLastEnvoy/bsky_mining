import requests
import json
import time
from datetime import datetime
import csv
from typing import List, Dict, Optional
import os
from getpass import getpass

class BlueskySearcher:
    def __init__(self):
        self.base_url = "https://bsky.social"
        self.session = requests.Session()
        self.access_token = None
        self.session.headers.update({
            'User-Agent': 'BlueskyAgroSearcher/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

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

    def search_posts(self, query: str, limit: int = 25, cursor: Optional[str] = None) -> Dict:
        """Busca posts usando a API autenticada do Bluesky"""
        if not self.access_token:
            print("❌ Erro: Não autenticado")
            return {}

        endpoint = f"{self.base_url}/xrpc/app.bsky.feed.searchPosts"

        params = {
            'q': query,
            'limit': min(limit, 25)
        }

        if cursor:
            params['cursor'] = cursor

        try:
            response = self.session.get(endpoint, params=params)

            if response.status_code == 429:
                print("⏳ Rate limit atingido. Aguardando 60s...")
                time.sleep(60)
                return self.search_posts(query, limit, cursor)

            if response.status_code == 403:
                print("❌ Acesso negado. Verifique suas credenciais.")
                return {}

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
            return {}

    def get_timeline_posts(self, limit: int = 50, cursor: Optional[str] = None) -> Dict:
        """Alternativa: busca posts do timeline público"""
        if not self.access_token:
            return {}

        endpoint = f"{self.base_url}/xrpc/app.bsky.feed.getTimeline"

        params = {'limit': min(limit, 50)}
        if cursor:
            params['cursor'] = cursor

        try:
            response = self.session.get(endpoint, params=params)

            if response.status_code == 429:
                print("⏳ Rate limit atingido. Aguardando 60s...")
                time.sleep(60)
                return self.get_timeline_posts(limit, cursor)

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição do timeline: {e}")
            return {}

    def extract_post_data(self, post: Dict) -> Dict:
        """Extrai dados relevantes de um post"""
        try:
            if 'post' in post:
                actual_post = post['post']
            else:
                actual_post = post

            record = actual_post.get('record', {})
            author = actual_post.get('author', {})

            return {
                'uri': actual_post.get('uri', ''),
                'cid': actual_post.get('cid', ''),
                'author_did': author.get('did', ''),
                'author_handle': author.get('handle', ''),
                'author_display_name': author.get('displayName', ''),
                'text': record.get('text', ''),
                'created_at': record.get('createdAt', ''),
                'reply_count': actual_post.get('replyCount', 0),
                'repost_count': actual_post.get('repostCount', 0),
                'like_count': actual_post.get('likeCount', 0),
                'indexed_at': actual_post.get('indexedAt', ''),
                'langs': record.get('langs', [])
            }
        except Exception as e:
            print(f"❌ Erro ao extrair dados: {e}")
            return {}

    def filter_posts_by_keyword(self, posts: List[Dict], keyword: str) -> List[Dict]:
        """Filtra posts que contêm a palavra-chave"""
        filtered_posts = []
        keyword_lower = keyword.lower()

        for post in posts:
            text = post.get('text', '').lower()
            if keyword_lower in text:
                filtered_posts.append(post)

        return filtered_posts

    def collect_posts(self, query: str, max_posts: int = 1000, delay: float = 2.0) -> List[Dict]:
        """Coleta posts com paginação"""
        all_posts = []
        cursor = None
        requests_made = 0

        print(f"🔍 Iniciando coleta de posts com '{query}'...")

        while len(all_posts) < max_posts:
            remaining = max_posts - len(all_posts)
            limit = min(25, remaining)

            print(f"📡 Request {requests_made + 1} - Coletados: {len(all_posts)}/{max_posts}")

            # Tenta busca primeiro
            result = self.search_posts(query, limit, cursor)

            if not result or 'posts' not in result:
                print("🔄 Busca direta falhou, tentando timeline...")
                result = self.get_timeline_posts(50, cursor)

                if not result or 'feed' not in result:
                    print("❌ Timeline também falhou")
                    break

                posts_raw = result.get('feed', [])
                using_timeline = True
            else:
                posts_raw = result.get('posts', [])
                using_timeline = False

            if not posts_raw:
                print("📭 Não há mais posts disponíveis")
                break

            # Processa os posts
            batch_posts = []
            for post_raw in posts_raw:
                post_data = self.extract_post_data(post_raw)
                if post_data and post_data.get('text'):
                    batch_posts.append(post_data)

            # Se usando timeline, filtra por palavra-chave
            if using_timeline:
                batch_posts = self.filter_posts_by_keyword(batch_posts, query)
                print(f"🎯 Filtrados {len(batch_posts)} posts com '{query}' neste lote")

            all_posts.extend(batch_posts)

            cursor = result.get('cursor')
            if not cursor:
                print("📄 Não há mais páginas disponíveis")
                break

            requests_made += 1

            if delay > 0:
                print(f"⏱️  Aguardando {delay}s...")
                time.sleep(delay)

        print(f"✅ Coleta finalizada! Total de posts coletados: {len(all_posts)}")
        return all_posts

    def save_to_csv(self, posts: List[Dict], filename: str = None):
        """Salva posts em arquivo CSV"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"bluesky_agro_posts_{timestamp}.csv"

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
            filename = f"bluesky_agro_posts_{timestamp}.json"

        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(posts, jsonfile, ensure_ascii=False, indent=2)

        print(f"💾 Posts salvos em: {filename}")

def load_env_file():
    """Carrega arquivo .env manualmente (sem dependências externas)"""
    env_path = '.env'
    env_vars = {}

    if os.path.exists(env_path):
        print("📄 Arquivo .env encontrado!")
        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Remove aspas se existirem
                        value = value.strip('"\'')
                        env_vars[key.strip()] = value
                        # Também define como variável de ambiente
                        os.environ[key.strip()] = value
            print("✅ Variáveis do .env carregadas!")
        except Exception as e:
            print(f"❌ Erro ao ler .env: {e}")
    else:
        print("📄 Arquivo .env não encontrado na pasta atual")

    return env_vars

def get_credentials():
    """Obtém credenciais de forma segura"""
    # Carrega arquivo .env
    load_env_file()

    # Credenciais fixas como fallback
    FIXED_EMAIL = "bsky.fprcx@slmail.me"
    FIXED_PASSWORD = "uj&$Nx$RqM5x@m$@DDJW"

    # Tenta variáveis de ambiente primeiro (incluindo as do .env)
    env_email = os.getenv('BLUESKY_EMAIL')
    env_password = os.getenv('BLUESKY_PASSWORD')

    print("🔐 Verificando credenciais...")

    if env_email and env_password:
        print("✅ Usando credenciais do arquivo .env")
        return env_email, env_password
    else:
        print("⚠️  Arquivo .env não encontrado ou incompleto")
        print("🔐 Opções disponíveis:")
        print("1 - Usar credenciais fixas (teste)")
        print("2 - Digitar credenciais manualmente")

        choice = input("Escolha uma opção (1-2): ").strip()

        if choice == "1":
            print("⚠️  USANDO CREDENCIAIS FIXAS")
            return FIXED_EMAIL, FIXED_PASSWORD
        else:
            email = input("📧 Digite seu email/handle: ").strip()
            password = getpass("🔑 Digite sua senha (oculta): ")
            return email, password

def main():
    """Função principal"""
    searcher = BlueskySearcher()

    # Configurações
    QUERY = "agro"
    MAX_POSTS = 500
    DELAY_BETWEEN_REQUESTS = 2.0

    print("🚀 === Coletor de Posts do Bluesky ===")
    print(f"🔍 Buscando por: '{QUERY}'")
    print(f"📊 Máximo de posts: {MAX_POSTS}")
    print(f"⏱️  Delay entre requests: {DELAY_BETWEEN_REQUESTS}s")
    print("-" * 50)

    # Obtém credenciais
    email, password = get_credentials()

    if not email or not password:
        print("❌ Credenciais não fornecidas")
        return

    # Autentica
    if not searcher.create_session(email, password):
        print("❌ Falha na autenticação")
        return

    try:
        # Coleta os posts
        posts = searcher.collect_posts(
            query=QUERY,
            max_posts=MAX_POSTS,
            delay=DELAY_BETWEEN_REQUESTS
        )

        if posts:
            print(f"\n📈 === Estatísticas ===")
            print(f"📝 Total de posts coletados: {len(posts)}")

            # Análise rápida
            total_likes = sum(post.get('like_count', 0) for post in posts)
            total_reposts = sum(post.get('repost_count', 0) for post in posts)

            print(f"💖 Total de likes: {total_likes}")
            print(f"🔄 Total de reposts: {total_reposts}")

            # Mostra exemplos
            print(f"\n🎯 === Primeiros 3 posts ===")
            for i, post in enumerate(posts[:3], 1):
                print(f"\n{i}. 👤 @{post['author_handle']}")
                print(f"   📄 Texto: {post['text'][:100]}...")
                print(f"   💖 Likes: {post['like_count']}, 🔄 Reposts: {post['repost_count']}")

            # Salva os arquivos
            print(f"\n💾 === Salvando arquivos ===")
            searcher.save_to_csv(posts)
            searcher.save_to_json(posts)

        else:
            print("❌ Nenhum post foi coletado.")

    except KeyboardInterrupt:
        print("\n\n⏹️  Interrompido pelo usuário.")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")

if __name__ == "__main__":
    main()

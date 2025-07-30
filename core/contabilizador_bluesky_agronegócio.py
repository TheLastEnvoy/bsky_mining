import requests
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Set
import os
from getpass import getpass
import re

class OptimizedBlueskyCounter2025:
    def __init__(self):
        self.base_url = "https://bsky.social"
        self.session = requests.Session()
        self.access_token = None
        self.refresh_token = None
        self.token_expires_at = None
        self.session.headers.update({
            'User-Agent': 'BlueskyAgroCounter2025/2.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })

        # Cache para evitar reprocessamento
        self.processed_uris: Set[str] = set()

        # Otimização: múltiplas queries para cobrir variações
        self.agro_queries = [
            "agronegócio", "agricultura", "pecuária", 
            "fazenda", "soja", "milho", "bovino"
        ]

        # Contadores otimizados
        self.stats = {
            'total_requests': 0,
            'total_posts_processed': 0,
            'duplicate_posts_skipped': 0,
            'posts_2025_count': 0,
            'posts_agronegocio_count': 0,
            'posts_brazil_count': 0,
            'posts_final_count': 0,
            'token_renewals': 0
        }

    def create_session(self, identifier: str, password: str) -> bool:
        """Cria sessão com gerenciamento aprimorado de tokens"""
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
            self.refresh_token = session_data.get('refreshJwt')

            # Estima expiração do token (5 minutos como segurança)
            self.token_expires_at = datetime.now() + timedelta(minutes=5)

            if self.access_token and self.refresh_token:
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}'
                })
                print("✅ Autenticação realizada com sucesso!")
                return True
            else:
                print("❌ Erro: Tokens não encontrados")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na autenticação: {e}")
            return False

    def refresh_session(self) -> bool:
        """Renova o token de acesso usando refresh token"""
        if not self.refresh_token:
            print("❌ Sem refresh token disponível")
            return False

        endpoint = f"{self.base_url}/xrpc/com.atproto.server.refreshSession"

        # Temporariamente usa o refresh token
        old_auth = self.session.headers.get('Authorization')
        self.session.headers.update({
            'Authorization': f'Bearer {self.refresh_token}'
        })

        try:
            response = self.session.post(endpoint)
            response.raise_for_status()

            session_data = response.json()
            new_access_token = session_data.get('accessJwt')
            new_refresh_token = session_data.get('refreshJwt')

            if new_access_token:
                self.access_token = new_access_token
                self.refresh_token = new_refresh_token or self.refresh_token
                self.token_expires_at = datetime.now() + timedelta(minutes=5)
                self.stats['token_renewals'] += 1

                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}'
                })
                print("🔄 Token renovado com sucesso!")
                return True
            else:
                print("❌ Erro na renovação do token")
                return False

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na renovação: {e}")
            # Restaura autorização anterior em caso de erro
            if old_auth:
                self.session.headers.update({'Authorization': old_auth})
            return False

    def ensure_valid_token(self) -> bool:
        """Garante que o token está válido"""
        if not self.token_expires_at or datetime.now() >= self.token_expires_at:
            print("⏰ Token próximo do vencimento, renovando...")
            return self.refresh_session()
        return True

    def search_posts_optimized(self, query: str, limit: int = 25, cursor: Optional[str] = None) -> Dict:
        """Busca otimizada com gerenciamento automático de tokens"""
        if not self.ensure_valid_token():
            print("❌ Falha na validação do token")
            return {}

        endpoint = f"{self.base_url}/xrpc/app.bsky.feed.searchPosts"

        # Otimização: query mais específica para reduzir ruído
        optimized_query = f'"{query}" lang:pt'

        params = {
            'q': optimized_query,
            'limit': min(limit, 25),
            'since': "2025-01-01T00:00:00Z",
            'until': "2025-12-31T23:59:59Z"
        }

        if cursor:
            params['cursor'] = cursor

        try:
            self.stats['total_requests'] += 1
            response = self.session.get(endpoint, params=params)

            if response.status_code == 429:
                # Otimização: backoff exponencial
                wait_time = min(60 * (2 ** (self.stats['total_requests'] % 4)), 300)
                print(f"⏳ Rate limit. Aguardando {wait_time}s (backoff exponencial)...")
                time.sleep(wait_time)
                return self.search_posts_optimized(query, limit, cursor)

            if response.status_code == 401:
                print("🔑 Token expirado, tentando renovar...")
                if self.refresh_session():
                    return self.search_posts_optimized(query, limit, cursor)
                else:
                    return {}

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")
            return {}

    def is_brazil_post_optimized(self, post: Dict) -> bool:
        """Detecção otimizada de posts brasileiros"""
        try:
            if 'post' in post:
                actual_post = post['post']
            else:
                actual_post = post

            record = actual_post.get('record', {})
            author = actual_post.get('author', {})
            text = record.get('text', '').lower()

            # 1. Verificação rápida: idioma português
            langs = record.get('langs', [])
            if any(lang.startswith('pt') for lang in langs):
                return True

            # 2. Handle brasileiro (.br)
            handle = author.get('handle', '').lower()
            if '.br' in handle or 'brasil' in handle:
                return True

            # 3. Padrões brasileiros otimizados (regex para performance)
            brazil_patterns = [
                r'\br\$\d', r'\breai[s]?\b', r'\bcpf\b', r'\bcnpj\b',
                r'\bbrasil\b', r'\bbrasileiro\b', r'\bbrasileira\b',
                r'\bs[ãa]o paulo\b', r'\brio de janeiro\b', r'\bbras[íi]lia\b'
            ]

            for pattern in brazil_patterns:
                if re.search(pattern, text):
                    return True

            # 4. Verificação de estados/regiões (só se necessário)
            if any(state in text for state in ['minas gerais', 'rio grande', 'santa catarina']):
                return True

            return False

        except Exception:
            return False

    def has_agro_keywords_optimized(self, text: str) -> bool:
        """Detecção otimizada de termos do agronegócio"""
        if not text:
            return False

        text_lower = text.lower()

        # Termos principais com regex para performance
        agro_patterns = [
            r'\bagroneg[óo]cio\b',
            r'\bagricultura\b', r'\bpecuária\b', r'\bfazenda\b',
            r'\b(soja|milho|algod[ãa]o|caf[ée]|cana)\b',
            r'\b(bovino|su[íi]no|avicultura)\b',
            r'\b(plantio|colheita|safra)\b'
        ]

        return any(re.search(pattern, text_lower) for pattern in agro_patterns)

    def process_multiple_queries(self, delay: float = 1.5, max_requests_per_query: int = 1000) -> Dict:
        """Executa múltiplas queries para cobertura completa"""
        print(f"🔍 Executando {len(self.agro_queries)} queries otimizadas...")

        start_time = datetime.now()

        for i, query in enumerate(self.agro_queries, 1):
            print(f"\n📡 Query {i}/{len(self.agro_queries)}: '{query}'")

            cursor = None
            requests_for_query = 0

            while requests_for_query < max_requests_per_query:
                result = self.search_posts_optimized(query, 25, cursor)

                if not result or 'posts' not in result:
                    break

                posts_raw = result.get('posts', [])
                if not posts_raw:
                    break

                # Processa lote
                self.process_posts_batch(posts_raw)

                requests_for_query += 1

                # Atualiza cursor
                cursor = result.get('cursor')
                if not cursor:
                    break

                # Rate limiting otimizado
                time.sleep(delay)

            print(f"   ✅ Concluída: {requests_for_query} requests")

        # Estatísticas finais
        elapsed = datetime.now() - start_time
        self.stats['elapsed_time'] = elapsed

        return self.stats

    def process_posts_batch(self, posts_raw: list):
        """Processa lote com deduplicação"""
        for post_raw in posts_raw:
            try:
                # Extrai URI para deduplicação
                if 'post' in post_raw:
                    actual_post = post_raw['post']
                else:
                    actual_post = post_raw

                post_uri = actual_post.get('uri', '')

                # Pula se já processado
                if post_uri in self.processed_uris:
                    self.stats['duplicate_posts_skipped'] += 1
                    continue

                self.processed_uris.add(post_uri)
                self.stats['total_posts_processed'] += 1

                # Extrai dados básicos
                record = actual_post.get('record', {})
                created_at = record.get('createdAt', '')
                text = record.get('text', '')

                # Aplica filtros otimizados
                is_2025 = self.is_post_from_2025(created_at)
                has_agro = self.has_agro_keywords_optimized(text)
                is_brazil = self.is_brazil_post_optimized(post_raw)

                # Contabiliza
                if is_2025:
                    self.stats['posts_2025_count'] += 1

                if has_agro:
                    self.stats['posts_agronegocio_count'] += 1

                if is_brazil:
                    self.stats['posts_brazil_count'] += 1

                if is_2025 and has_agro and is_brazil:
                    self.stats['posts_final_count'] += 1

            except Exception as e:
                print(f"⚠️ Erro ao processar post: {e}")
                continue

    def is_post_from_2025(self, post_date: str) -> bool:
        """Verifica se o post é de 2025"""
        try:
            if not post_date:
                return False
            post_datetime = datetime.fromisoformat(post_date.replace('Z', '+00:00'))
            return post_datetime.year == 2025
        except Exception:
            return False

    def print_optimized_report(self):
        """Relatório otimizado com métricas de performance"""
        stats = self.stats

        print(f"\n🚀 === RELATÓRIO OTIMIZADO ===")
        print(f"🕐 Tempo total: {stats.get('elapsed_time', 'N/A')}")
        print(f"📡 Total de requests: {stats['total_requests']:,}")
        print(f"🔄 Renovações de token: {stats['token_renewals']}")
        print(f"📊 Posts únicos processados: {stats['total_posts_processed']:,}")
        print(f"🔄 Posts duplicados ignorados: {stats['duplicate_posts_skipped']:,}")

        if stats['total_requests'] > 0:
            efficiency = stats['total_posts_processed'] / stats['total_requests']
            print(f"⚡ Eficiência: {efficiency:.1f} posts únicos/request")

        print(f"\n🎯 === RESULTADO FINAL ===")
        print(f"✅ Posts agronegócio Brasil 2025: {stats['posts_final_count']:,}")

        if stats['total_posts_processed'] > 0:
            success_rate = (stats['posts_final_count'] / stats['total_posts_processed']) * 100
            print(f"📊 Taxa de precisão: {success_rate:.3f}%")

def main():
    """Função principal otimizada"""
    counter = OptimizedBlueskyCounter2025()

    print("🚀 === CONTADOR OTIMIZADO AGRONEGÓCIO BRASIL 2025 ===")
    print("✨ Melhorias implementadas:")
    print("   🔄 Renovação automática de tokens")
    print("   🎯 Múltiplas queries para maior cobertura")
    print("   ⚡ Deduplicação de posts")
    print("   📊 Detecção otimizada de Brasil/agronegócio")
    print("   ⏱️ Backoff exponencial para rate limits")
    print("-" * 60)

    # Obtém credenciais
    load_env_file()
    email = os.getenv('BLUESKY_EMAIL') or input("📧 Email: ").strip()
    password = os.getenv('BLUESKY_PASSWORD') or getpass("🔑 Senha: ")

    if not counter.create_session(email, password):
        print("❌ Falha na autenticação")
        return

    try:
        print(f"\n🏁 Iniciando contagem otimizada...")
        time.sleep(2)

        # Executa contagem com múltiplas queries
        final_stats = counter.process_multiple_queries(
            delay=1.5,  # Menor delay devido às otimizações
            max_requests_per_query=2000
        )

        # Relatório final
        counter.print_optimized_report()

    except KeyboardInterrupt:
        print("\n⏹️ Interrompido pelo usuário.")
        counter.print_optimized_report()
    except Exception as e:
        print(f"❌ Erro: {e}")

def load_env_file():
    """Carrega .env"""
    if os.path.exists('.env'):
        with open('.env', 'r', encoding='utf-8') as f:
            for line in f:
                if '=' in line and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value.strip('"\'')

if __name__ == "__main__":
    main()

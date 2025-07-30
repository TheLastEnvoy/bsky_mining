# Bluesky Agronegócio 2025 - Contador Otimizado

Este projeto realiza a contagem e análise de posts relacionados ao agronegócio brasileiro na plataforma Bluesky durante o ano de 2025. O script utiliza autenticação via API, múltiplas queries otimizadas e técnicas de deduplicação para garantir precisão e eficiência.

## Funcionalidades
- Autenticação automática e renovação de tokens
- Busca otimizada por termos do agronegócio
- Detecção de posts brasileiros
- Deduplicação de posts para evitar contagem duplicada
- Relatórios detalhados de desempenho e resultados
- Suporte a variáveis de ambiente via `.env`

## Como usar
## Como adaptar para outros temas

Você pode usar os scripts para levantar posts sobre qualquer tema no Bluesky, não apenas agronegócio. Para isso, basta alterar as palavras-chave ou queries nos scripts principais:

- **core/contabilizador_bluesky_agronegócio.py**
  - Altere a lista `self.agro_queries` para os termos desejados:
    ```python
    self.agro_queries = [
        "palavra1", "palavra2", "palavra3"
    ]
    ```

- **core/bsky_agro2025_analyze.py**
  - Altere o valor da variável `QUERY` na função `main()` para o termo desejado:
    ```python
    QUERY = "seu_tema_aqui"
    ```

Você pode adicionar quantos termos quiser, adaptando para qualquer área de interesse. Os scripts vão buscar, filtrar e analisar os posts conforme os novos temas definidos.
1. Clone o repositório e instale as dependências necessárias (requer Python 3.8+ e `requests`).
2. Crie um arquivo `.env` com as variáveis `BLUESKY_EMAIL` e `BLUESKY_PASSWORD`.
3. Execute o script principal:
   ```bash
   python contabilizador_bluesky_agronegócio.py
   ```
4. Siga as instruções no terminal para iniciar a contagem.

## Estrutura dos arquivos
- Pasta `core/`: scripts principais do projeto (ex: contabilizador_bluesky_agronegócio.py, sentiment_analyzer3.py, organiser_csv2.py, bsky_agro2025_analyze.py).
- Pasta `data/`: arquivos de dados (csv, json, txt, ods, etc).
- `.env`: (não versionado) Armazena credenciais de acesso.
- `example.env`: Exemplo de arquivo de variáveis de ambiente. Use como modelo para criar o seu `.env`, substituindo com seu e-mail e senha do Bluesky.

## Observações
- O arquivo `.env` **NÃO** deve ser versionado. Certifique-se de que está listado no `.gitignore`.
- O script implementa backoff exponencial para lidar com limites de requisições da API.

## Licença
Seja feliz.
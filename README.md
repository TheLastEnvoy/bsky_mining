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
1. Clone o repositório e instale as dependências necessárias (requer Python 3.8+ e `requests`).
2. Crie um arquivo `.env` com as variáveis `BLUESKY_EMAIL` e `BLUESKY_PASSWORD`.
3. Execute o script principal:
   ```bash
   python contabilizador_bluesky_agronegócio.py
   ```
4. Siga as instruções no terminal para iniciar a contagem.

## Estrutura dos arquivos
- `contabilizador_bluesky_agronegócio.py`: Script principal de contagem e análise.
- `sentiment_analyzer3.py`: Analisa o sentimento dos posts utilizando modelos transformers da Hugging Face.
- `organiser_csv2.py`: Organiza arquivos CSV para outros formatos como XLSX, TXT ou um novo CSV já organizado.
- `.env`: (não versionado) Armazena credenciais de acesso.
- `example.env`: Exemplo de arquivo de variáveis de ambiente. Use como modelo para criar o seu `.env`, substituindo com seu e-mail e senha do Bluesky.
- Outros arquivos: Dados de entrada/saída e scripts auxiliares.

## Observações
- O arquivo `.env` **NÃO** deve ser versionado. Certifique-se de que está listado no `.gitignore`.
- O script implementa backoff exponencial para lidar com limites de requisições da API.

## Licença
Seja feliz.
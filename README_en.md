# Bluesky Agribusiness 2025 - Optimized Counter

**[Read this in Portuguese](README.md)**

This project counts and analyzes posts related to Brazilian agribusiness on the Bluesky platform during 2025. The script uses API authentication, multiple optimized queries, and deduplication techniques for accuracy and efficiency.

## Features
- Automatic authentication and token renewal
- Optimized search for agribusiness terms
- Detection of Brazilian posts
- Deduplication to avoid double counting
- Detailed performance and result reports
- Support for environment variables via `.env`

## How to Use
1. Clone the repository and install the required dependencies (Python 3.8+ and `requests`).
2. Create a `.env` file with the variables `BLUESKY_EMAIL` and `BLUESKY_PASSWORD`.
3. Run the main script:
   ```bash
   python core/contabilizador_bluesky_agronegócio.py
   ```
4. Follow the terminal instructions to start counting.

## How to Adapt for Other Topics

You can use the scripts to collect posts about any topic on Bluesky, not just agribusiness. To do this, change the keywords or queries in the main scripts:

- **core/contabilizador_bluesky_agronegócio.py**
  - Edit the `self.agro_queries` list with your desired terms:
    ```python
    self.agro_queries = [
        "keyword1", "keyword2", "keyword3"
    ]
    ```
- **core/bsky_agro2025_analyze.py**
  - Change the `QUERY` variable in the `main()` function:
    ```python
    QUERY = "your_topic_here"
    ```

You can add as many terms as you want, adapting to any area of interest. The scripts will search, filter, and analyze posts according to the new topics.

## File Structure
- `core/`: main project scripts (e.g., contabilizador_bluesky_agronegócio.py, sentiment_analyzer3.py, organiser_csv2.py, bsky_agro2025_analyze.py)
- `data/`: data files (csv, json, txt, ods, etc)
- `.env`: (not versioned) Stores access credentials
- `example.env`: Example environment file. Use it as a template to create your own `.env`, replacing with your Bluesky email and password.

## Notes
- The `.env` file **MUST NOT** be versioned. Make sure it is listed in `.gitignore`.
- The script implements exponential backoff to handle API rate limits.

## License
Be happy.

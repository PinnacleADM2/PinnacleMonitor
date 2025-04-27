# Projeto Crypto Feed -> Webhook

Este projeto consiste em dois scripts Python para coletar dados de criptomoedas:

1.  `catalog_assets.py`: Cataloga todos os pares de negociação (símbolos) de diversas exchanges usando a biblioteca CCXT e os armazena em um banco de dados PostgreSQL.
2.  `stream_and_webhook.py`: Conecta-se aos feeds WebSocket de várias exchanges usando a biblioteca Cryptofeed, recebe dados de trades e tickers em tempo real, e envia esses dados para um endpoint de webhook especificado.

## Pré-requisitos

*   Um servidor (Droplet Ubuntu, por exemplo) com acesso à internet.
*   Python 3.10 ou superior instalado.
*   Um banco de dados PostgreSQL acessível (local ou remoto).
*   Um endpoint de webhook para receber os dados.

## Configuração no Servidor Ubuntu

1.  **Atualizar e instalar pacotes:**

    ```bash
    sudo apt update && sudo apt install -y python3.10 python3-venv git postgresql postgresql-contrib
    ```
    *Nota: A instalação do `postgresql` é opcional se você estiver usando um banco de dados remoto.*

2.  **Clonar o repositório (ou copiar os arquivos):**
    *   Se você colocou este projeto em um repositório Git:
        ```bash
        git clone <url_do_seu_repositorio>
        cd <nome_do_diretorio_do_repositorio>
        ```
    *   Senão, copie os arquivos (`catalog_assets.py`, `stream_and_webhook.py`, `requirements.txt`) para um diretório no servidor.

3.  **Criar e ativar um ambiente virtual:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

4.  **Atualizar pip e instalar dependências:**

    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```

5.  **Configurar o Banco de Dados PostgreSQL (Exemplo Local):**
    *   Se instalou o PostgreSQL localmente, configure um usuário e um banco de dados:
        ```bash
        sudo -u postgres psql
        CREATE DATABASE crypto_catalog;
        CREATE USER crypto_user WITH PASSWORD 'sua_senha_segura';
        GRANT ALL PRIVILEGES ON DATABASE crypto_catalog TO crypto_user;
        \q
        ```
    *   Teste a conexão:
        ```bash
        psql -h localhost -U crypto_user -d crypto_catalog
        ```

6.  **Configurar Variáveis de Ambiente:**
    É crucial definir as seguintes variáveis de ambiente antes de executar os scripts. Você pode defini-las diretamente no terminal, em um arquivo `.env` (usando `python-dotenv`, que já é uma dependência do `cryptofeed`), ou através de gerenciadores de sistema como `systemd`.

    ```bash
    export DATABASE_URL="postgresql://crypto_user:sua_senha_segura@localhost/crypto_catalog" # Ajuste se o DB for remoto
    export WEBHOOK_URL="https://seu-endpoint.com/webhook"
    ```

## Execução

Certifique-se de que o ambiente virtual está ativado (`source venv/bin/activate`) e as variáveis de ambiente estão definidas.

1.  **Catalogar Ativos (Execute uma vez ou periodicamente):**
    Este script buscará os símbolos de todas as exchanges suportadas pelo CCXT e os salvará no banco de dados.

    ```bash
    python catalog_assets.py
    ```
    *   Isso pode levar algum tempo e gerar muitos logs, dependendo do número de exchanges e da sua conexão.

2.  **Iniciar o Streaming e Webhook:**
    Este script se conectará aos feeds das exchanges configuradas no `cryptofeed` e enviará os dados recebidos para o seu `WEBHOOK_URL`.

    ```bash
    python stream_and_webhook.py
    ```
    *   O script continuará rodando indefinidamente, consumindo os feeds. Use `Ctrl+C` para parar.
    *   Monitore os logs para verificar se as conexões estão ativas e se os webhooks estão sendo enviados.

## Observações

*   **Gerenciamento de Símbolos:** O script `stream_and_webhook.py` atualmente tenta usar a lista de símbolos padrão de cada exchange (`EXCHANGE_MAP[ex].symbols()`). Isso pode não ser completo ou atualizado. Uma abordagem mais robusta seria modificar o `stream_and_webhook.py` para ler os símbolos do banco de dados populado pelo `catalog_assets.py` para cada exchange.
*   **Robustez:** Para produção, considere executar os scripts usando um gerenciador de processos como `systemd` ou `supervisor` para garantir que eles reiniciem automaticamente em caso de falha.
*   **Segurança:** Proteja suas credenciais de banco de dados e o endpoint do webhook.
*   **Limites de Taxa:** Esteja ciente dos limites de taxa (rate limits) das APIs das exchanges, especialmente ao executar `catalog_assets.py`. 
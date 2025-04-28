import ccxt, os
# Adiciona imports para dotenv
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, String, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import pprint # Para imprimir o dicionário de ambiente

# --- DEBUGGING --- 
print("DEBUG: Tentando carregar .env")
# Carrega variáveis do arquivo .env
dotenv_loaded = load_dotenv()
print(f"DEBUG: load_dotenv() retornou: {dotenv_loaded}")
# --- FIM DEBUGGING ---

# URI do Postgres via env var (agora deve funcionar)
DB_URL = os.getenv("DATABASE_URL")

# --- DEBUGGING ---
print(f"DEBUG: Valor de DB_URL após os.getenv: {DB_URL}")
print("DEBUG: Verificando algumas variáveis de ambiente:")
print(f"DEBUG: USER={os.getenv('USER')}")
print(f"DEBUG: HOME={os.getenv('HOME')}")
# Imprime a variável específica que queremos, caso exista diretamente no os.environ
print(f"DEBUG: os.environ.get('DATABASE_URL'): {os.environ.get('DATABASE_URL')}") 
# Atenção: Imprimir todo o os.environ pode vazar informações sensíveis se houver outras variáveis.
# Vamos imprimir apenas a chave que nos interessa diretamente do dicionário os.environ.
# print("DEBUG: Conteúdo parcial de os.environ:") 
# pprint.pprint({k: v for k, v in os.environ.items() if k in ['HOME', 'PATH', 'USER', 'DATABASE_URL']})
# --- FIM DEBUGGING ---

# Verifica se DB_URL foi carregado corretamente
if not DB_URL:
    print("Erro Crítico: DATABASE_URL não encontrada. Verifique o arquivo .env e se python-dotenv está instalado.")
    exit(1)

engine = create_engine(DB_URL)
meta = MetaData()

assets = Table(
    "assets", meta,
    Column("exchange", String, primary_key=True),
    Column("symbol",   String, primary_key=True),
)

meta.create_all(engine)

def catalogar_todos():
    conn = engine.connect()
    # Use a dialect-specific insert statement for ON CONFLICT DO NOTHING
    from sqlalchemy.dialects.postgresql import insert

    print("Iniciando a catalogação de ativos...")
    for ex_id in ccxt.exchanges:
        print(f"Processando exchange: {ex_id}")
        try:
            ex_class = getattr(ccxt, ex_id)
            # Adiciona configuração para desabilitar rate limit (pode ser útil para catalogação, mas use com cuidado)
            ex = ex_class({'enableRateLimit': False})
            
            # --- Verificação Segura de Requisitos ---
            # Verifica se 'requires' existe e se 'fetchMarkets' está presente
            fetch_markets_requires_auth = False
            fetch_markets_emulated = False
            if hasattr(ex, 'requires') and isinstance(ex.requires, dict):
                if ex.requires.get('fetchMarkets') == 'emulated':
                    fetch_markets_emulated = True
                # Verifica se fetchMarkets é True (ou não presente, assumindo que não requer auth)
                # E se API keys são necessárias
                if ex.requires.get('fetchMarkets', False) is True and not (ex.apiKey and ex.secret):
                    fetch_markets_requires_auth = True
            
            # Pula se emulado ou se requer auth não fornecida
            if fetch_markets_emulated:
                print(f"Pulando {ex_id}: fetchMarkets é emulado.")
                continue
            if fetch_markets_requires_auth:
                print(f"Pulando {ex_id}: Requer API key/secret não fornecida para fetchMarkets.")
                continue
            # --- Fim Verificação Segura ---

            markets = ex.load_markets()
            print(f"Encontrados {len(markets)} mercados para {ex_id}")
            if markets: # Procede apenas se houver mercados
                insert_stmt = insert(assets).values([
                    {"exchange": ex_id, "symbol": symbol} for symbol in markets
                ])
                # Especifica a ação ON CONFLICT DO NOTHING (específico do PostgreSQL)
                do_nothing_stmt = insert_stmt.on_conflict_do_nothing(
                    index_elements=['exchange', 'symbol']
                )
                conn.execute(do_nothing_stmt)
                print(f"Ativos para {ex_id} inseridos/atualizados.")
            else:
                print(f"Nenhum mercado encontrado para {ex_id}.")


        except ccxt.NetworkError as e:
            print(f"Erro de rede ao acessar {ex_id}: {e}")
        except ccxt.ExchangeError as e:
            print(f"Erro da exchange {ex_id}: {e}")
        except Exception as e:
            print(f"Falha inesperada em {ex_id}: {type(e).__name__} - {e}")
            # Para depuração, pode ser útil ver o traceback completo:
            # import traceback
            # traceback.print_exc()
        finally:
            # Garante que a conexão com a exchange seja fechada se aplicável
            if 'ex' in locals() and hasattr(ex, 'close'):
                asyncio.run(ex.close()) # Fecha a conexão async se existir

    print("Catalogação concluída.")
    conn.commit() # Commita a transação
    conn.close()

if __name__ == "__main__":
    # A verificação de DB_URL já foi feita acima após load_dotenv()
    # if not DB_URL:
    #     print("Erro: A variável de ambiente DATABASE_URL não está definida.")
    #     exit(1)
    catalogar_todos()
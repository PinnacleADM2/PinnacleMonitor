import ccxt, os
from sqlalchemy import create_engine, Column, String, Table, MetaData

# URI do Postgres via env var
DB_URL = os.getenv("DATABASE_URL")
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
            ex = ex_class()
            # Algumas exchanges requerem autenticação ou chaves de API para carregar mercados,
            # vamos pular essas por enquanto ou adicionar tratamento de erro específico se necessário.
            if ex.requires['fetchMarkets'] == 'emulated':
                 print(f"Pulando {ex_id}: fetchMarkets é emulado.")
                 continue
            if ex.requires['fetchMarkets'] and not (ex.apiKey and ex.secret):
                print(f"Pulando {ex_id}: Requer API key/secret não fornecida.")
                continue

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
    if not DB_URL:
        print("Erro: A variável de ambiente DATABASE_URL não está definida.")
        exit(1)
    catalogar_todos()
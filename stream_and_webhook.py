import os
import asyncio
import aiohttp
import logging
from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, TICKER
from cryptofeed.exchanges import EXCHANGE_MAP

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # ex: https://meu-dominio.com/webhook

# Criar uma única sessão aiohttp para reutilização
SESSION = None

async def get_session():
    """Retorna a sessão aiohttp, criando uma se necessário."""
    global SESSION
    if SESSION is None or SESSION.closed:
        SESSION = aiohttp.ClientSession()
    return SESSION

async def post_webhook(data):
    """Envia dados para o webhook de forma assíncrona."""
    if not WEBHOOK_URL:
        logger.warning("WEBHOOK_URL não está definida. Não é possível enviar dados.")
        return

    session = await get_session()
    try:
        async with session.post(WEBHOOK_URL, json=data, timeout=aiohttp.ClientTimeout(total=5)) as response:
            response.raise_for_status() # Levanta exceção para respostas 4xx/5xx
            logger.debug(f"Webhook enviado com sucesso: {data['feed']}/{data['pair']}")
    except asyncio.TimeoutError:
        logger.error(f"Timeout ao enviar webhook para {data['feed']}/{data['pair']}")
    except aiohttp.ClientError as e:
        logger.error(f"Erro ao enviar webhook para {data['feed']}/{data['pair']}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao enviar webhook: {e}")


async def trade_cb(trade, receipt_timestamp):
    """Callback para eventos de trade."""
    data = {
        "feed": trade.exchange,
        "pair": trade.symbol,
        "order_id": trade.id,
        "side": trade.side,
        "amount": float(trade.amount), # Converter Decimal para float para JSON
        "price": float(trade.price),   # Converter Decimal para float para JSON
        "timestamp": trade.timestamp,
        "receipt_timestamp": receipt_timestamp
    }
    await post_webhook(data)

async def ticker_cb(ticker, receipt_timestamp):
    """Callback para eventos de ticker."""
    data = {
        "feed": ticker.exchange,
        "pair": ticker.symbol,
        "bid": float(ticker.bid), # Converter Decimal para float para JSON
        "ask": float(ticker.ask), # Converter Decimal para float para JSON
        "timestamp": ticker.timestamp,
        "receipt_timestamp": receipt_timestamp
    }
    await post_webhook(data)

async def main():
    """Função principal para configurar e executar o FeedHandler."""
    if not WEBHOOK_URL:
        logger.error("A variável de ambiente WEBHOOK_URL deve ser definida.")
        return

    logger.info("Iniciando FeedHandler...")
    fh = FeedHandler()

    # Itera sobre todas as exchanges disponíveis no cryptofeed
    # NOTA: ex_cls.symbols() pode não retornar todos os símbolos dinamicamente.
    # Uma abordagem mais robusta seria carregar os símbolos do banco de dados
    # populado por catalog_assets.py para cada exchange.
    exchanges_to_subscribe = []
    for ex_name, ex_cls in EXCHANGE_MAP.items():
        try:
            # Verifica se a exchange suporta os canais desejados
            supported_channels = ex_cls.supported_channels() if hasattr(ex_cls, 'supported_channels') else [TRADES, TICKER] # Supõe suporte se não especificado
            channels_to_add = []
            if TRADES in supported_channels:
                channels_to_add.append(TRADES)
            if TICKER in supported_channels:
                channels_to_add.append(TICKER)

            if not channels_to_add:
                logger.warning(f"Exchange {ex_name} não suporta TRADES nem TICKER. Pulando.")
                continue

            # Obtem lista de símbolos. Trata exceções potenciais.
            symbols = ex_cls.symbols() if hasattr(ex_cls, 'symbols') else []
            if not symbols:
                 # Tenta buscar símbolos comuns se a lista específica não estiver disponível
                 common_symbols = ['BTC-USDT', 'ETH-USDT', 'BTC-USD', 'ETH-USD']
                 logger.warning(f"Lista de símbolos vazia ou não disponível para {ex_name}. Tentando símbolos comuns: {common_symbols}")
                 symbols = common_symbols

            if symbols:
                logger.info(f"Adicionando feed para {ex_name} com {len(symbols)} símbolo(s) e canais {channels_to_add}.")
                fh.add_feed(
                    ex_cls(symbols=symbols, channels=channels_to_add, callbacks={TRADES: trade_cb, TICKER: ticker_cb})
                )
                exchanges_to_subscribe.append(ex_name)
            else:
                logger.warning(f"Não foi possível obter símbolos para {ex_name}. Pulando.")

        except Exception as e:
            logger.error(f"Erro ao configurar feed para {ex_name}: {e}")

    if not exchanges_to_subscribe:
         logger.error("Nenhuma exchange configurada para subscrição. Verifique logs.")
         return

    logger.info(f"Iniciando feeds para: {', '.join(exchanges_to_subscribe)}")
    await fh.run()
    logger.info("FeedHandler encerrado.")

    # Fecha a sessão aiohttp ao final
    global SESSION
    if SESSION:
        await SESSION.close()
        logger.info("Sessão aiohttp fechada.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário.")
    finally:
        # Garante que a sessão aiohttp seja fechada em caso de exceção não tratada no main()
        if SESSION and not SESSION.closed:
            asyncio.run(SESSION.close())
            logger.info("Sessão aiohttp fechada no bloco finally.")
import os
import asyncio
import aiohttp
import logging
import time
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict

from sqlalchemy import create_engine, select, MetaData, Table

from cryptofeed import FeedHandler
from cryptofeed.defines import TRADES, TICKER, LIQUIDATIONS, L2_BOOK
from cryptofeed.exchanges import EXCHANGE_MAP
from cryptofeed.callback import TradeCallback, TickerCallback, BookCallback, LiquidationCallback

# --- Configuração de Logging ---
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(), # Para saída no console
        # logging.FileHandler("crypto_monitor.log") # Descomente para logar em arquivo
    ]
)
logger = logging.getLogger(__name__)

# --- Configuração Global ---
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
DATABASE_URL = os.getenv("DATABASE_URL")

if not WEBHOOK_URL:
    logger.error("Variável de ambiente WEBHOOK_URL não definida!")
    exit(1)
if not DATABASE_URL:
    logger.error("Variável de ambiente DATABASE_URL não definida!")
    exit(1)

# Top Exchanges (Exemplo - ajuste conforme necessário)
TOP_EXCHANGES = [
    'BINANCE', 'BYBIT', 'OKX', 'KRAKEN', 'KUCOIN', 
    'GATEIO', 'BITGET', 'HUOBI', 'COINBASE', 'BITFINEX'
    # Adicione ou remova exchanges conforme necessário
]

# Para garantir nomes consistentes com cryptofeed (geralmente maiúsculas)
TOP_EXCHANGES = [ex.upper() for ex in TOP_EXCHANGES]

# --- Gerenciamento de Estado --- 
# Estrutura: symbol_states[exchange][symbol] = { ... dados ... }
symbol_states = defaultdict(lambda: defaultdict(dict))
# Exemplo de dados por símbolo:
# 'last_price': float
# 'last_update_ts': float (timestamp unix)
# 'trades_3min': deque([(timestamp, price, amount_usd)]) # Guarda trades (ts, price, valor_usd)
# 'volume_3min_usd': float # Volume em USDT calculado periodicamente
# 'price_history_15min': deque([(timestamp, price)]) # Guarda preços (ts, price)
# 'price_history_30min': deque([(timestamp, price)]) # Guarda preços (ts, price)
# 'daily_volume_usd': float # Volume acumulado desde meia-noite UTC
# 'daily_open_price': float # Preço de abertura à meia-noite UTC
# 'daily_high_price': float
# 'daily_low_price': float
# 'max_daily_volume_usd': float # Máximo volume diário já registrado pelo script
# 'is_daily_reset': bool # Flag para indicar se o reset diario ocorreu

# Sessão aiohttp global
SESSION = None

# --- Funções Auxiliares ---

async def get_session():
    """Retorna a sessão aiohttp global, criando-a se necessário."""
    global SESSION
    if SESSION is None or SESSION.closed:
        SESSION = aiohttp.ClientSession()
        logger.info("Sessão aiohttp criada.")
    return SESSION

async def close_session():
    """Fecha a sessão aiohttp global se ela existir."""
    global SESSION
    if SESSION and not SESSION.closed:
        await SESSION.close()
        logger.info("Sessão aiohttp fechada.")
        SESSION = None

def get_db_engine():
    """Cria e retorna uma engine SQLAlchemy."""
    try:
        engine = create_engine(DATABASE_URL)
        # Testa a conexão (opcional, mas bom para feedback rápido)
        with engine.connect() as connection:
            logger.info("Conexão com o banco de dados estabelecida com sucesso.")
        return engine
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise

def load_initial_symbols(engine, target_exchanges):
    """Carrega símbolos do banco de dados para as exchanges alvo."""
    metadata = MetaData()
    assets_table = Table('assets', metadata, autoload_with=engine)
    symbols_by_exchange = defaultdict(list)
    exchange_count = 0

    logger.info(f"Carregando símbolos do DB para exchanges: {', '.join(target_exchanges)}")
    try:
        with engine.connect() as connection:
            query = select(assets_table.c.exchange, assets_table.c.symbol)
            # Filtra pelas exchanges alvo, ignorando case
            query = query.where(assets_table.c.exchange.ilike(sqlalchemy.sql.any_(target_exchanges)))
            result = connection.execute(query)
            
            count = 0
            processed_exchanges = set()
            for row in result:
                ex_name_db = row.exchange # Nome como está no DB
                # Encontra o nome canônico da exchange (maiúsculo)
                ex_name_canonical = next((ex for ex in target_exchanges if ex.lower() == ex_name_db.lower()), None)
                if ex_name_canonical:
                    symbol = row.symbol
                    symbols_by_exchange[ex_name_canonical].append(symbol)
                    if ex_name_canonical not in processed_exchanges:
                         processed_exchanges.add(ex_name_canonical)
                         exchange_count += 1
                    count += 1
                else:
                    logger.warning(f"Exchange '{ex_name_db}' do DB não encontrada na lista TOP_EXCHANGES.")
                    
            logger.info(f"Carregados {count} símbolos de {exchange_count} exchanges do banco de dados.")
            if count == 0:
                 logger.warning("Nenhum símbolo carregado do DB. Verifique a tabela 'assets' e a lista TOP_EXCHANGES.")
        return symbols_by_exchange
    except Exception as e:
        logger.error(f"Erro ao carregar símbolos do banco de dados: {e}")
        return defaultdict(list) # Retorna vazio em caso de erro


async def post_webhook(trigger_type: str, data: dict):
    """Envia dados formatados para o webhook."""
    session = await get_session()
    payload = {
        "trigger_type": trigger_type,
        "timestamp_event": datetime.now(timezone.utc).isoformat(),
        **data # Adiciona os dados específicos do gatilho
    }
    try:
        async with session.post(WEBHOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status >= 300:
                 logger.error(f"Erro ao enviar webhook ({trigger_type}): {response.status} - {await response.text()}")
            else:
                 logger.info(f"Webhook enviado com sucesso: {trigger_type} - {data.get('exchange', '')}/{data.get('symbol', '')}")
            response.raise_for_status() # Levanta exceção para respostas 4xx/5xx
    except asyncio.TimeoutError:
        logger.error(f"Timeout ao enviar webhook para {trigger_type} - {data.get('exchange', '')}/{data.get('symbol', '')}")
    except aiohttp.ClientError as e:
        logger.error(f"Erro de cliente ao enviar webhook ({trigger_type}): {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao enviar webhook ({trigger_type}): {e}")

# --- Callbacks (Serão implementados/modificados em breve) ---

def _get_usd_value(exchange: str, symbol: str, price: float, amount: float) -> float | None:
    """Estima o valor em USD de um trade. Retorna None se não for possível estimar."""
    # Pares diretos com USD/Stablecoins
    stable_quotes = ['USDT', 'USD', 'USDC', 'BUSD', 'TUSD', 'DAI', 'PAX']
    try:
        base, quote = symbol.split('-') # Assumindo formato BASE-QUOTE
        if quote in stable_quotes:
            return float(price * amount)
        # TODO: Adicionar lógica para pares como BTC/ETH? 
        # Exigiria buscar o preço atual de ETH/USD ou BTC/USD
        # Por enquanto, retornamos None para simplificar.
        else:
            # logger.debug(f"Não foi possível calcular valor USD para {exchange} {symbol}")
            return None
    except ValueError:
        # logger.warning(f"Formato de símbolo inválido para cálculo USD: {exchange} {symbol}")
        return None
    except Exception as e:
        logger.error(f"Erro ao calcular valor USD para {exchange} {symbol}: {e}")
        return None

class TradeHandler(TradeCallback):
    async def __call__(self, trade, receipt_timestamp):
        # trade fields: .exchange, .symbol, .id, .side, .price, .amount, .timestamp
        exchange = trade.exchange
        symbol = trade.symbol
        timestamp = trade.timestamp
        price = float(trade.price)
        amount = float(trade.amount)
        
        if price <= 0 or amount <= 0:
            # logger.debug(f"Trade ignorado (preço/quantidade inválidos): {exchange} {symbol}")
            return

        state = symbol_states[exchange][symbol]
        now_ts = time.time()
        
        # --- Garante Inicialização Diária (caso Ticker não tenha chegado primeiro) ---
        current_day = datetime.fromtimestamp(now_ts, timezone.utc).date()
        state_day = datetime.fromtimestamp(state.get('last_update_ts', 0), timezone.utc).date()
        if not state or current_day > state_day:
            logger.info(f"Inicializando/Resetando estado diário (via Trade) para: {exchange} {symbol} (Preço: {price:.4f})")
            state['daily_open_price'] = price
            state['daily_high_price'] = price
            state['daily_low_price'] = price
            state['daily_volume_usd'] = 0.0
            state['max_daily_volume_usd'] = state.get('daily_volume_usd', 0.0)
            state['is_daily_reset'] = True
            if 'price_history_15min' not in state:
                state['price_history_15min'] = deque(maxlen=900) 
            if 'price_history_30min' not in state:
                state['price_history_30min'] = deque(maxlen=1800)
            if 'trades_3min' not in state:
                 state['trades_3min'] = deque(maxlen=200) 
        
        # --- Atualização de Estado ---
        state['last_price'] = price # Atualiza último preço também com trades
        state['last_update_ts'] = timestamp

        # Calcula valor em USD (se possível)
        value_usd = _get_usd_value(exchange, symbol, price, amount)

        if value_usd is not None:
            # Adiciona à fila de trades recentes (timestamp, price, value_usd)
            # Usamos o valor USD aqui para facilitar o cálculo de volume depois
            if 'trades_3min' in state:
                 state['trades_3min'].append((timestamp, price, value_usd))
            else: # Caso raro onde estado foi criado mas deque não
                 state['trades_3min'] = deque([(timestamp, price, value_usd)], maxlen=200)
            
            # Acumula volume diário
            state['daily_volume_usd'] = state.get('daily_volume_usd', 0.0) + value_usd
            state['daily_high_price'] = max(state.get('daily_high_price', price), price)
            state['daily_low_price'] = min(state.get('daily_low_price', price), price)
            
            # Limpa flag de reset após a primeira atualização do dia
            if state.get('is_daily_reset'):
                 state['is_daily_reset'] = False 

            # --- Verificação de Gatilho Imediato (Trade Grande) ---
            # Para mercados futuros/derivativos, um trade individual > 100k pode ser interessante
            # Precisamos diferenciar Spot de Futuros (ex: pelo nome da exchange ou símbolo)
            # Exemplo SIMPLES: (precisa refinar)
            is_future = 'FUTURES' in exchange or 'PERP' in symbol or 'SWAP' in symbol 
            if is_future and value_usd > 100000: # Limiar de 100k USD
                 trigger_data = {
                     "exchange": exchange,
                     "symbol": symbol,
                     "price_at_trigger": price,
                     "trade_id": trade.id,
                     "side": trade.side,
                     "amount": amount,
                     "value_usd": round(value_usd, 2),
                     "trade_timestamp": timestamp
                 }
                 logger.warning(f"GATILHO: Trade Individual Grande Detectado - {exchange} {symbol} - Valor: {value_usd:.2f} USD")
                 await post_webhook("large_individual_trade", trigger_data)
                 
            # logger.debug(f"Trade State Updated: {exchange} {symbol} VolDay: {state['daily_volume_usd']:.2f} USD")
        # else:
            # logger.debug(f"Trade ignorado para stats (valor USD não calculado): {exchange} {symbol}")


class TickerHandler(TickerCallback):
    async def __call__(self, ticker, receipt_timestamp):
        # ticker fields: .exchange, .symbol, .bid, .ask, .timestamp
        exchange = ticker.exchange
        symbol = ticker.symbol
        timestamp = ticker.timestamp # Timestamp do evento da exchange
        now_ts = time.time() # Timestamp local

        # Usa o preço médio entre bid/ask se ambos existirem, senão o que existir
        price = (ticker.bid + ticker.ask) / 2 if ticker.bid > 0 and ticker.ask > 0 else ticker.bid or ticker.ask
        
        if price <= 0: # Ignora tickers sem preço válido
             # logger.debug(f"Ticker ignorado (preço inválido): {exchange} {symbol}")
             return

        state = symbol_states[exchange][symbol]

        # --- Inicialização/Reset Diário ---
        current_day = datetime.fromtimestamp(now_ts, timezone.utc).date()
        state_day = datetime.fromtimestamp(state.get('last_update_ts', 0), timezone.utc).date()

        if not state or current_day > state_day:
            logger.info(f"Inicializando/Resetando estado diário para: {exchange} {symbol} (Preço: {price:.4f})")
            state['daily_open_price'] = price
            state['daily_high_price'] = price
            state['daily_low_price'] = price
            state['daily_volume_usd'] = 0.0 # Resetar volume vindo dos trades
            state['max_daily_volume_usd'] = state.get('daily_volume_usd', 0.0) # Mantem max anterior ou reseta
            state['is_daily_reset'] = True # Marca que o reset ocorreu
            # Inicializa deques se não existirem
            if 'price_history_15min' not in state:
                state['price_history_15min'] = deque(maxlen=900) 
            if 'price_history_30min' not in state:
                state['price_history_30min'] = deque(maxlen=1800)
            if 'trades_3min' not in state:
                 state['trades_3min'] = deque(maxlen=200) # Mantém este
            # Remove o de 10min se existir de execuções anteriores
            if 'price_history_10min' in state:
                del state['price_history_10min']

        # --- Atualização de Estado ---
        state['last_price'] = float(price)
        state['last_update_ts'] = timestamp 

        # Atualiza High/Low diário
        state['daily_high_price'] = max(state['daily_high_price'], price)
        state['daily_low_price'] = min(state['daily_low_price'], price)

        # Adiciona ao histórico de preços para cálculo de % de mudança
        # Garante que as deques existem antes de adicionar
        if 'price_history_15min' in state:
             state['price_history_15min'].append((timestamp, float(price)))
        if 'price_history_30min' in state:
             state['price_history_30min'].append((timestamp, float(price)))
        
        # Limpa flag de reset após a primeira atualização do dia
        if state.get('is_daily_reset'):
             state['is_daily_reset'] = False 
             
        # logger.debug(f"Ticker State Updated: {exchange} {symbol} Price: {state['last_price']:.4f}")
        

class LiquidationHandler(LiquidationCallback):
    async def __call__(self, liquidation, receipt_timestamp):
        # liquidation fields: .exchange, .symbol, .side, .price, .amount, .timestamp, .order_id (opcional)
        exchange = liquidation.exchange
        symbol = liquidation.symbol
        price = float(liquidation.price)
        amount = float(liquidation.amount)
        timestamp = liquidation.timestamp
        side = liquidation.side # Lado da posição liquidada (Buy/Sell)
        order_id = liquidation.order_id # Pode ser None

        if price <= 0 or amount <= 0:
            # logger.debug(f"Liquidação ignorada (preço/quantidade inválidos): {exchange} {symbol}")
            return

        # Calcula o valor liquidado em USD (se possível)
        value_usd = _get_usd_value(exchange, symbol, price, amount)
        
        if value_usd is not None:
            logger.info(f"Liquidação Detectada: {exchange} {symbol} | Lado: {side} | Quant: {amount} | Preço: {price} | Valor USD: {value_usd:.2f}")
            
            # --- Verificação de Gatilho (Liquidação > 50k USD) ---
            if value_usd > 50000: # Limiar de 50k USD
                trigger_data = {
                    "exchange": exchange,
                    "symbol": symbol,
                    "price_at_trigger": price,
                    "liquidation_side": side,
                    "amount_liquidated": amount,
                    "value_usd": round(value_usd, 2),
                    "order_id": order_id,
                    "liquidation_timestamp": timestamp
                }
                logger.warning(f"GATILHO: Liquidação Grande Detectada - {exchange} {symbol} - Valor: {value_usd:.2f} USD")
                await post_webhook("large_liquidation", trigger_data)
        else:
            logger.info(f"Liquidação Detectada (sem valor USD): {exchange} {symbol} | Lado: {side} | Quant: {amount} | Preço: {price}")
            # Poderíamos ainda assim enviar um webhook sem o valor USD se desejado


# --- Tarefas Periódicas ---

async def check_3min_volume_task():
    """Verifica periodicamente o volume de trades nos últimos 3 minutos."""
    logger.info("Iniciando task: check_3min_volume_task (a cada 60s)")
    while True:
        await asyncio.sleep(60) # Roda a cada 60 segundos
        now_ts = time.time()
        three_minutes_ago = now_ts - (3 * 60)
        
        #logger.debug("Executando verificação de volume 3min...")
        triggered_symbols = [] # Para evitar spam se muitos alertas ocorrerem juntos
        
        # Itera sobre uma cópia das chaves para evitar erro se dict mudar durante iteração
        exchanges = list(symbol_states.keys())
        for exchange in exchanges:
            symbols = list(symbol_states[exchange].keys())
            for symbol in symbols:
                state = symbol_states[exchange].get(symbol)
                if not state or 'trades_3min' not in state:
                    continue
                
                trades_queue = state['trades_3min']
                current_volume_3min_usd = 0.0
                valid_trades_in_queue = []
                
                # Calcula volume recente e remove trades muito antigos da fila
                # Iteramos na ordem em que foram adicionados (mais antigo primeiro)
                for trade_ts, trade_price, trade_value_usd in list(trades_queue): # Itera sobre cópia
                    if trade_ts >= three_minutes_ago:
                        current_volume_3min_usd += trade_value_usd
                        valid_trades_in_queue.append((trade_ts, trade_price, trade_value_usd))
                
                # Atualiza a deque apenas com os trades válidos (otimização)
                # Embora a maxlen já limite, isso garante precisão de tempo
                # state['trades_3min'] = deque(valid_trades_in_queue, maxlen=trades_queue.maxlen)
                # Nota: Reconstruir a deque pode ser menos eficiente que deixar maxlen cuidar.
                # Vamos confiar no maxlen e na verificação de timestamp por enquanto.
                
                # Atualiza o valor calculado no estado (para referência/debug, opcional)
                state['volume_3min_usd'] = current_volume_3min_usd
                
                # --- Verificação de Gatilho (Volume > 500k USD) ---
                if current_volume_3min_usd > 500000:
                    # Evita alertas repetidos muito rapidamente para o mesmo símbolo
                    # Podemos adicionar um timestamp do último alerta no estado
                    last_alert_ts = state.get('last_volume_alert_ts', 0)
                    if now_ts - last_alert_ts > (5 * 60): # Só alerta a cada 5 minutos no máximo
                        trigger_data = {
                            "exchange": exchange,
                            "symbol": symbol,
                            "price_at_trigger": state.get('last_price', 0.0),
                            "volume_3min_usd": round(current_volume_3min_usd, 2),
                            "check_timestamp": now_ts
                        }
                        logger.warning(f"GATILHO: Volume Anormal 3min Detectado - {exchange} {symbol} - Vol: {current_volume_3min_usd:.2f} USD")
                        await post_webhook("abnormal_3min_volume", trigger_data)
                        state['last_volume_alert_ts'] = now_ts # Atualiza timestamp do último alerta
                        triggered_symbols.append(f"{exchange}-{symbol}")

        # if triggered_symbols:
        #      logger.debug(f"Verificação de volume 3min concluída. Gatilhos: {', '.join(triggered_symbols)}")
        # else:
        #      logger.debug("Verificação de volume 3min concluída. Nenhum gatilho.")


async def check_price_change_task(time_window_minutes: int, percent_threshold: float):
    """Verifica periodicamente a mudança percentual de preço em uma janela de tempo.
    
    Args:
        time_window_minutes: A janela de tempo em minutos (ex: 15, 30).
        percent_threshold: O limiar percentual para disparar o alerta (ex: 5.0 para 5%).
    """
    logger.info(f"Iniciando task: check_{time_window_minutes}min_change_task (a cada 60s, threshold: {percent_threshold}%)")
    history_key = f"price_history_{time_window_minutes}min"
    alert_type = f"price_change_{time_window_minutes}min"
    last_alert_ts_key = f"last_change_{time_window_minutes}min_alert_ts"
    
    while True:
        await asyncio.sleep(60) # Roda a cada 60 segundos
        now_ts = time.time()
        window_start_ts = now_ts - (time_window_minutes * 60)
        
        # logger.debug(f"Executando verificação de mudança {time_window_minutes}min...")
        triggered_symbols = []
        
        exchanges = list(symbol_states.keys())
        for exchange in exchanges:
            symbols = list(symbol_states[exchange].keys())
            for symbol in symbols:
                state = symbol_states[exchange].get(symbol)
                if not state or history_key not in state or not state[history_key]:
                    continue
                
                price_queue = state[history_key]
                current_price = state.get('last_price')
                
                if not current_price:
                    continue

                # Encontra o preço mais antigo dentro da janela de tempo
                oldest_price_in_window = None
                # A deque está ordenada do mais antigo para o mais novo
                for p_ts, p_price in price_queue: 
                    if p_ts >= window_start_ts:
                        oldest_price_in_window = p_price
                        break # Encontrou o primeiro (mais antigo) na janela
                
                if oldest_price_in_window is None or oldest_price_in_window <= 0:
                    continue # Não há dados suficientes na janela ou preço inválido

                # Calcula a mudança percentual
                percent_change = ((current_price - oldest_price_in_window) / oldest_price_in_window) * 100
                
                # --- Verificação de Gatilho --- 
                if abs(percent_change) >= percent_threshold:
                    last_alert_ts = state.get(last_alert_ts_key, 0)
                    # Limita alertas a cada X minutos (ex: 10 min para mudanças)
                    if now_ts - last_alert_ts > (10 * 60): 
                        trigger_data = {
                            "exchange": exchange,
                            "symbol": symbol,
                            "current_price": current_price,
                            f"price_{time_window_minutes}min_ago": oldest_price_in_window,
                            f"percent_change_{time_window_minutes}min": round(percent_change, 2),
                            "check_timestamp": now_ts
                        }
                        log_level = logging.WARNING if percent_change > 0 else logging.ERROR
                        logger.log(log_level, f"GATILHO: Mudança Preço {time_window_minutes}min - {exchange} {symbol} - Change: {percent_change:.2f}%")
                        await post_webhook(alert_type, trigger_data)
                        state[last_alert_ts_key] = now_ts
                        triggered_symbols.append(f"{exchange}-{symbol} ({percent_change:.1f}%)")
                        
        # if triggered_symbols:
        #     logger.debug(f"Verificação de mudança {time_window_minutes}min concluída. Gatilhos: {', '.join(triggered_symbols)}")
        # else:
        #      logger.debug(f"Verificação de mudança {time_window_minutes}min concluída. Nenhum gatilho.")

async def check_daily_stats_task():
    """Verifica periodicamente se o volume diário atingiu um novo máximo."""
    logger.info("Iniciando task: check_daily_stats_task (a cada 5 min)")
    while True:
        # Roda com menos frequência, a cada 5 minutos
        await asyncio.sleep(5 * 60) 
        now_ts = time.time()
        
        # logger.debug("Executando verificação de máximo volume diário...")
        triggered_symbols = []
        
        exchanges = list(symbol_states.keys())
        for exchange in exchanges:
            symbols = list(symbol_states[exchange].keys())
            for symbol in symbols:
                state = symbol_states[exchange].get(symbol)
                # Precisa ter volume diário e o máximo anterior registrado
                if not state or 'daily_volume_usd' not in state or 'max_daily_volume_usd' not in state:
                    continue
                    
                current_daily_volume = state['daily_volume_usd']
                max_volume_so_far = state['max_daily_volume_usd']
                
                # --- Verificação de Gatilho (Novo Máximo Diário) ---
                # Verifica se o volume atual é significativamente maior que o máximo anterior
                # (Evita alertas por volumes iniciais pequenos)
                if current_daily_volume > max_volume_so_far and current_daily_volume > 10000: # Exige um volume mínimo para alertar
                    state['max_daily_volume_usd'] = current_daily_volume # Atualiza o máximo
                    
                    trigger_data = {
                        "exchange": exchange,
                        "symbol": symbol,
                        "price_at_trigger": state.get('last_price', 0.0),
                        "new_max_daily_volume_usd": round(current_daily_volume, 2),
                        "check_timestamp": now_ts
                    }
                    logger.warning(f"GATILHO: Novo Máximo Volume Diário - {exchange} {symbol} - Vol: {current_daily_volume:.2f} USD")
                    await post_webhook("new_max_daily_volume", trigger_data)
                    triggered_symbols.append(f"{exchange}-{symbol} ({current_daily_volume:.0f})")
        
        # if triggered_symbols:
        #      logger.debug(f"Verificação de máximo volume diário concluída. Gatilhos: {', '.join(triggered_symbols)}")


async def check_top_movers_task():
    """Identifica periodicamente os 30 maiores ganhadores e perdedores diários."""
    logger.info("Iniciando task: check_top_movers_task (a cada 15 min)")
    NUM_MOVERS = 30 # Top/Bottom N
    MIN_DAILY_VOLUME_THRESHOLD = 50000 # Ignora ativos com volume diário muito baixo
    RELEVANT_ASSET_COUNT = 200 # Calcula a mudança para os N de maior volume

    while True:
        await asyncio.sleep(15 * 60) # Roda a cada 15 minutos
        now_ts = time.time()
        
        # logger.debug("Executando verificação de top movers...")
        
        all_assets_data = []
        # 1. Coleta dados relevantes de todos os ativos com estado
        exchanges = list(symbol_states.keys())
        for exchange in exchanges:
            symbols = list(symbol_states[exchange].keys())
            for symbol in symbols:
                state = symbol_states[exchange].get(symbol)
                if (state and 
                    state.get('last_price') and 
                    state.get('daily_open_price') and 
                    state.get('daily_volume_usd') is not None and
                    state['daily_open_price'] > 0 and # Evita divisão por zero
                    state['daily_volume_usd'] >= MIN_DAILY_VOLUME_THRESHOLD):
                    
                    daily_change_pct = ((state['last_price'] - state['daily_open_price']) 
                                        / state['daily_open_price']) * 100
                    
                    all_assets_data.append({
                        "exchange": exchange,
                        "symbol": symbol,
                        "daily_volume_usd": state['daily_volume_usd'],
                        "daily_change_pct": daily_change_pct,
                        "last_price": state['last_price']
                    })

        if not all_assets_data:
            # logger.debug("Sem dados suficientes para calcular top movers.")
            continue

        # 2. Seleciona os N mais relevantes por volume
        # Ordena por volume diário (decrescente) e pega os primeiros N
        top_volume_assets = sorted(all_assets_data, key=lambda x: x['daily_volume_usd'], reverse=True)[:RELEVANT_ASSET_COUNT]

        if not top_volume_assets:
            continue
            
        # 3. Ordena os relevantes por mudança percentual
        sorted_by_change = sorted(top_volume_assets, key=lambda x: x['daily_change_pct'], reverse=True)
        
        # 4. Extrai Top Ganhadores e Perdedores
        top_gainers = sorted_by_change[:NUM_MOVERS]
        top_losers = sorted_by_change[-NUM_MOVERS:]
        # Inverte a lista de perdedores para que o maior perdedor (-%) venha primeiro
        top_losers.reverse()

        # 5. Envia Webhook
        if top_gainers or top_losers:
            trigger_data = {
                "check_timestamp": now_ts,
                "top_gainers": [
                    {"exchange": g['exchange'], "symbol": g['symbol'], "change_pct": round(g['daily_change_pct'], 2), "price": g['last_price']}
                    for g in top_gainers
                ],
                "top_losers": [
                    {"exchange": l['exchange'], "symbol": l['symbol'], "change_pct": round(l['daily_change_pct'], 2), "price": l['last_price']}
                    for l in top_losers
                ]
            }
            logger.info(f"GATILHO: Top Movers Diários - Gainers: {len(top_gainers)}, Losers: {len(top_losers)}")
            await post_webhook("daily_top_movers", trigger_data)
        # else:
            # logger.debug("Verificação de top movers concluída. Nenhuma mudança significativa encontrada.")


async def daily_reset_task():
    """Reseta as estatísticas diárias de todos os símbolos à meia-noite UTC."""
    logger.info("Iniciando task: daily_reset_task")
    # Reimporta timedelta aqui se necessario ou ajuste a logica
    while True:
        now = datetime.now(timezone.utc)
        # Calcula segundos até a próxima meia-noite UTC
        midnight = datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + timedelta(days=1)
        sleep_seconds = (midnight - now).total_seconds()
        logger.info(f"Próximo reset diário de estatísticas em {sleep_seconds:.0f} segundos.")
        await asyncio.sleep(sleep_seconds + 5) # Dorme até um pouco depois da meia-noite
        
        logger.warning(f"Executando reset diário de estatísticas ({datetime.now(timezone.utc)})...")
        reset_count = 0
        exchanges = list(symbol_states.keys())
        for exchange in exchanges:
            symbols = list(symbol_states[exchange].keys())
            for symbol in symbols:
                state = symbol_states[exchange].get(symbol)
                if state:
                    # Guarda o último preço como preço de abertura do novo dia
                    new_open_price = state.get('last_price')
                    if new_open_price and new_open_price > 0:
                        state['daily_open_price'] = new_open_price
                        state['daily_high_price'] = new_open_price
                        state['daily_low_price'] = new_open_price
                    else: # Se não tiver preço válido, reseta para None ou 0?
                        state['daily_open_price'] = None 
                        state['daily_high_price'] = None
                        state['daily_low_price'] = None
                        
                    # Reseta volumes e flags
                    state['daily_volume_usd'] = 0.0
                    state['max_daily_volume_usd'] = 0.0 # Reseta o máximo também
                    state['is_daily_reset'] = True # Indica que o reset ocorreu
                    
                    # Limpa as filas de histórico? Opcional, elas se auto-limpam com maxlen
                    # state.get('trades_3min', deque(maxlen=200)).clear()
                    # state.get('price_history_15min', deque(maxlen=900)).clear()
                    # state.get('price_history_30min', deque(maxlen=1800)).clear()
                    reset_count += 1
                    
        logger.warning(f"Reset diário concluído para {reset_count} símbolos.")
        # Pequeno sleep extra para evitar rodar duas vezes se houver atraso
        await asyncio.sleep(60)


# --- Função Principal ---

async def main():
    engine = get_db_engine() # Cria a engine (levanta erro se falhar)
    initial_symbols = load_initial_symbols(engine, TOP_EXCHANGES)

    if not initial_symbols:
        logger.error("Nenhum símbolo inicial carregado. Encerrando.")
        return

    # Instanciar Callbacks
    trade_handler = TradeHandler()
    ticker_handler = TickerHandler()
    liquidation_handler = LiquidationHandler()

    # Configurar FeedHandler
    # Nota: Precisamos verificar quais exchanges suportam LIQUIDATIONS
    # A configuração exata de feeds/canais/símbolos pode precisar de ajustes
    config = {'log': {'level': 'WARNING'}} # Reduzir verbosidade do log do cryptofeed
    fh = FeedHandler(config=config)

    subscribed_exchanges = set()
    for ex_name, symbols in initial_symbols.items():
        if not symbols:
            continue
        
        ex_class = EXCHANGE_MAP.get(ex_name)
        if not ex_class:
            logger.warning(f"Exchange {ex_name} não encontrada no EXCHANGE_MAP do cryptofeed. Pulando.")
            continue

        # Define os canais a serem subscritos
        channels = [TRADES, TICKER]
        callbacks = {TRADES: trade_handler, TICKER: ticker_handler}
        
        # Verifica suporte a LIQUIDATIONS (exemplo, a lista real pode variar)
        # Consulte a documentação do cryptofeed para suporte exato
        if ex_name in ['BINANCEFUTURES', 'BYBIT', 'FTX', 'BITMEX', 'OKEX', 'HUOBI']: # Adicionar outros se aplicável
             # Nota: Cryptofeed usa nomes específicos como BINANCEFUTURES
             # Precisamos mapear nosso TOP_EXCHANGES para os nomes do cryptofeed
             # E verificar se a classe suporta o canal
             if LIQUIDATIONS in ex_class.defines. LIKELY_SUPPORTED_CHANNELS: # Checagem básica
                logger.info(f"Adicionando canal LIQUIDATIONS para {ex_name}")
                channels.append(LIQUIDATIONS)
                callbacks[LIQUIDATIONS] = liquidation_handler
             else:
                 logger.warning(f"Exchange {ex_name} listada como suportando LIQUIDATIONS, mas classe não confirma. Verifique.")
        
        # Limita o número de símbolos por exchange se necessário (ex: top 100)
        # symbols_to_add = symbols[:100] # Exemplo: só os primeiros 100 por agora
        symbols_to_add = symbols # Por enquanto, todos os carregados

        logger.info(f"Configurando feed para {ex_name} com {len(symbols_to_add)} símbolos e canais {channels}")
        try:
            # Usando a instanciação direta da classe da exchange
            fh.add_feed(ex_class(channels=channels, symbols=symbols_to_add, callbacks=callbacks))
            subscribed_exchanges.add(ex_name)
        except Exception as e:
            logger.error(f"Erro ao adicionar feed para {ex_name}: {e}")

    if not subscribed_exchanges:
        logger.error("Nenhuma exchange foi subscrita com sucesso. Encerrando.")
        return
    
    logger.info(f"Iniciando FeedHandler para: {', '.join(subscribed_exchanges)}")
    
    # Iniciar tarefas periódicas em background
    tasks = [
        asyncio.create_task(check_3min_volume_task()),
        asyncio.create_task(check_price_change_task(time_window_minutes=15, percent_threshold=5.0)),
        asyncio.create_task(check_price_change_task(time_window_minutes=30, percent_threshold=5.0)),
        asyncio.create_task(check_daily_stats_task()),
        asyncio.create_task(check_top_movers_task()),
        asyncio.create_task(daily_reset_task()),
    ]
    logger.info(f"{len(tasks)} tarefas periódicas iniciadas.")

    try:
        # fh.run() é bloqueante, então não precisamos de await aqui se for a última coisa
        await fh.run()
    finally:
        logger.warning("FeedHandler encerrado ou encontrou um erro. Cancelando tarefas...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True) # Espera cancelamento
        await close_session() # Fecha a sessão aiohttp
        logger.info("Tarefas canceladas e sessão fechada.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execução interrompida pelo usuário (Ctrl+C).")
    except Exception as e:
         logger.critical(f"Erro crítico não tratado no main: {e}", exc_info=True)
    finally:
        # Garante o fechamento da sessão em caso de erro na inicialização do asyncio.run(main())
        if SESSION and not SESSION.closed:
             logger.warning("Fechando sessão aiohttp no bloco finally externo.")
             asyncio.run(close_session())
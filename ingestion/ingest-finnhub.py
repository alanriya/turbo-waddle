# Ingest Data from finnhub, publish to batch layer as parquet file
import websocket

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("Closed Websocket")

def on_open(ws):
    print("Opening Websocket Connections")
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=cm1fh01r01qt5f3oov30cm1fh01r01qt5f3oov3g",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
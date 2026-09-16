import asyncio
import json
import os
from datetime import datetime

import pytz
from websockets import connect
from termcolor import cprint


# ============================================================
# TRACKING WHALE ORDERS
# Binance Large Trade Detector
# ============================================================

# Cryptos to track
symbols = [
    "btcusdt",
    "ethusdt",
    "solusdt",
    "bnbusdt",
    "dogeusdt",
    "wifusdt",
]

# Binance WebSocket
websocket_url_base = "wss://stream.binance.com:9443/ws/"

# CSV file
trades_filename = "binance_trades.csv"

# Minimum trade size displayed
MIN_USD_SIZE = 15_000


# ============================================================
# CREATE CSV FILE
# ============================================================

if not os.path.isfile(trades_filename):
    with open(trades_filename, "w", encoding="utf-8") as f:
        f.write(
            "Event Time,"
            "Symbol,"
            "Price,"
            "Quantity,"
            "USD Size,"
            "Trade Type,"
            "Aggregate Trade ID,"
            "First Trade ID,"
            "Trade Time,"
            "Is Buyer Maker\n"
        )


# ============================================================
# BINANCE TRADE STREAM
# ============================================================

async def binance_trade_stream(uri, symbol, filename):

    while True:

        try:

            async with connect(uri) as websocket:

                cprint(
                    f"Connected to {symbol.upper()}",
                    "cyan",
                )

                while True:

                    # Receive trade from Binance
                    message = await websocket.recv()
                    trade = json.loads(message)

                    # ====================================================
                    # TRADE DATA
                    # ====================================================

                    price = float(trade["p"])
                    quantity = float(trade["q"])

                    aggregate_trade_id = trade["a"]
                    first_trade_id = trade["f"]

                    is_buyer_maker = trade["m"]

                    # ====================================================
                    # USD VALUE
                    # ====================================================

                    usd_size = price * quantity

                    # ====================================================
                    # TIME
                    # ====================================================

                    event_time = datetime.fromtimestamp(
                        trade["E"] / 1000,
                        tz=pytz.UTC,
                    ).strftime("%Y-%m-%d %H:%M:%S")

                    trade_time = datetime.fromtimestamp(
                        trade["T"] / 1000,
                        tz=pytz.UTC,
                    ).strftime("%Y-%m-%d %H:%M:%S")

                    # Keep EST for compatibility with original script
                    est = pytz.timezone("US/Eastern")

                    readable_trade_time = datetime.fromtimestamp(
                        trade["T"] / 1000,
                        tz=est,
                    ).strftime("%Y-%m-%d %H:%M:%S")

                    # Compact time for terminal
                    display_time = datetime.fromtimestamp(
                        trade["T"] / 1000,
                        tz=est,
                    ).strftime("%H:%M:%S")

                    # ====================================================
                    # LARGE TRADE FILTER
                    # ====================================================

                    if usd_size >= MIN_USD_SIZE:

                        # ------------------------------------------------
                        # BUY / SELL
                        # ------------------------------------------------
                        #
                        # is_buyer_maker = False
                        # Buyer is aggressive -> BUY
                        #
                        # is_buyer_maker = True
                        # Seller is aggressive -> SELL
                        #

                        trade_type = (
                            "SELL"
                            if is_buyer_maker
                            else "BUY"
                        )

                        # Remove USDT for terminal display
                        display_symbol = (
                            symbol
                            .upper()
                            .replace("USDT", "")
                        )

                        # =================================================
                        # DEFAULT APPEARANCE
                        # =================================================

                        marker = ""
                        attrs = []

                        if trade_type == "SELL":
                            color = "red"
                        else:
                            color = "green"

                        # =================================================
                        # WHALE LEVELS
                        # =================================================

                        # $1,000,000+
                        if usd_size >= 1_000_000:

                            marker = "***"
                            attrs = ["bold"]

                            if trade_type == "SELL":
                                color = "magenta"
                            else:
                                color = "blue"

                        # $500,000+
                        elif usd_size >= 500_000:

                            marker = "**"
                            attrs = ["bold"]

                            if trade_type == "SELL":
                                color = "magenta"
                            else:
                                color = "blue"

                        # $100,000+
                        elif usd_size >= 100_000:

                            marker = "*"
                            attrs = ["bold"]

                            if trade_type == "SELL":
                                color = "magenta"
                            else:
                                color = "blue"

                        # $50,000+
                        elif usd_size >= 50_000:

                            attrs = ["bold"]

                        # =================================================
                        # TERMINAL OUTPUT
                        # Trading Tape Style
                        # =================================================

                        output = (
                            f"{marker:<4}"
                            f"{trade_type:<5} "
                            f"{display_symbol:<5} "
                            f"{display_time} "
                            f"${usd_size:>12,.0f}"
                        )

                        cprint(
                            output,
                            color,
                            attrs=attrs,
                        )

                        # =================================================
                        # SAVE FULL DATA TO CSV
                        # =================================================

                        with open(
                            filename,
                            "a",
                            encoding="utf-8",
                        ) as f:

                            f.write(
                                f"{event_time},"
                                f"{symbol.upper()},"
                                f"{price},"
                                f"{quantity},"
                                f"{usd_size:.2f},"
                                f"{trade_type},"
                                f"{aggregate_trade_id},"
                                f"{first_trade_id},"
                                f"{trade_time},"
                                f"{is_buyer_maker}\n"
                            )

        # ================================================================
        # CONNECTION ERROR / AUTO RECONNECT
        # ================================================================

        except Exception as e:

            cprint(
                f"\nConnection error {symbol.upper()}: {e}",
                "yellow",
            )

            cprint(
                f"Reconnecting {symbol.upper()} in 5 seconds...\n",
                "yellow",
            )

            await asyncio.sleep(5)


# ============================================================
# MAIN
# ============================================================

async def main():

    tasks = []

    for symbol in symbols:

        uri = (
            f"{websocket_url_base}"
            f"{symbol}@aggTrade"
        )

        tasks.append(
            binance_trade_stream(
                uri,
                symbol,
                trades_filename,
            )
        )

    await asyncio.gather(*tasks)


# ============================================================
# START PROGRAM
# ============================================================

if __name__ == "__main__":
    asyncio.run(main())
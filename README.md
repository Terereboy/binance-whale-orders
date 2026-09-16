# Binance Whale Orders

Whale Orders is a small Python terminal application that monitors large cryptocurrency trades in real time. It connects to Binance's public aggregate-trade WebSocket streams, highlights trades above the configured USD threshold, and records matching trades in `binance_trades.csv`.

## Requirements

- Python 3.11 or newer
- Internet access to Binance's public WebSocket service

No Binance account or API keys are required.

## Installation

Clone or download the project, open a terminal in its directory, and create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Usage

Run:

```bash
python whale_orders.py
```

The program displays qualifying trades in the terminal and creates `binance_trades.csv` in the current working directory. Stop it with `Ctrl+C`.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

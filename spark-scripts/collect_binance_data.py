"""
Coleta dados de criptomoedas da API pública da Binance.
Salva no formato JSON no MinIO (camada bronze/raw).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import requests
import json
from datetime import datetime
import sys


def fetch_binance_ticker(symbols):
    """
    Busca dados de ticker da Binance para os símbolos especificados.
    
    Args:
        symbols: Lista de pares (ex: ['BTCUSDT', 'ETHUSDT'])
    
    Returns:
        Lista de dicionários com dados de ticker
    """
    print(f"📡 Fetching data from Binance API...")
    
    base_url = "https://api.binance.com/api/v3/ticker/24hr"
    all_data = []
    
    for symbol in symbols:
        try:
            response = requests.get(f"{base_url}?symbol={symbol}", timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Adiciona timestamp de coleta
            data['collected_at'] = datetime.now().isoformat()
            all_data.append(data)
            
            print(f"✅ {symbol}: ${float(data['lastPrice']):.2f}")
            
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {str(e)}")
    
    return all_data


def run(output_path):
    """
    Executa a coleta de dados e salva no MinIO.
    
    Args:
        output_path: Path S3 para salvar dados (ex: s3a://bronze/crypto/)
    """
    print(f"🚀 Starting Binance Data Collection")
    print(f"💾 Output: {output_path}")
    
    # Símbolos para coletar (principais criptos)
    symbols = [
        'BTCUSDT',   # Bitcoin
        'ETHUSDT',   # Ethereum
        'BNBUSDT',   # Binance Coin
        'ADAUSDT',   # Cardano
        'SOLUSDT',   # Solana
        'XRPUSDT',   # Ripple
        'DOTUSDT',   # Polkadot
        'DOGEUSDT',  # Dogecoin
    ]
    
    # Coleta dados da API
    data = fetch_binance_ticker(symbols)
    
    if not data:
        print("❌ No data collected!")
        sys.exit(1)
    
    print(f"\n📊 Total records collected: {len(data)}")
    
    # Cria sessão Spark
    spark = SparkSession.builder \
        .appName("Binance Data Collector") \
        .getOrCreate()
    
    try:
        # Converte para DataFrame
        df = spark.createDataFrame(data)
        
        # Adiciona metadados
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        # Mostra preview
        print("\n📋 Data preview:")
        df.select("symbol", "lastPrice", "volume", "priceChangePercent").show(10, truncate=False)
        
        # Define path com particionamento por data
        date_str = datetime.now().strftime("%Y-%m-%d")
        final_path = f"{output_path}/date={date_str}"
        
        # Salva no MinIO em formato JSON (camada bronze)
        df.write \
            .mode("overwrite") \
            .json(final_path)
        
        print(f"\n✅ Data saved to: {final_path}")
        print(f"📦 Format: JSON (Bronze layer)")
        print("✨ Collection completed successfully!")
        
    except Exception as e:
        print(f"❌ Error saving data: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        output_path = "s3a://bronze/crypto/binance"
        print(f"⚠️  No output path provided, using default: {output_path}")
    else:
        output_path = sys.argv[1]
    
    run(output_path)
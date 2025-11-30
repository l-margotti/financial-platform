from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import requests
from datetime import datetime
import sys


def fetch_binance_ticker(symbols):
    """Busca dados da Binance"""
    print(f"📡 Fetching data from Binance API...")
    base_url = "https://api.binance.com/api/v3/ticker/24hr"
    all_data = []
    
    for symbol in symbols:
        try:
            response = requests.get(f"{base_url}?symbol={symbol}", timeout=10)
            response.raise_for_status()
            data = response.json()
            data['collected_at'] = datetime.now().isoformat()
            all_data.append(data)
            print(f"✅ {symbol}: ${float(data['lastPrice']):.2f}")
        except Exception as e:
            print(f"❌ Error fetching {symbol}: {str(e)}")
    
    return all_data


def run(output_path):
    """Executa coleta e salva no MinIO"""
    print(f"🚀 Starting Binance Data Collection")
    
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'ADAUSDT', 
               'SOLUSDT', 'XRPUSDT', 'DOTUSDT', 'DOGEUSDT']
    
    data = fetch_binance_ticker(symbols)
    
    if not data:
        print("❌ No data collected!")
        sys.exit(1)
    
    spark = SparkSession.builder.appName("Binance Collector").getOrCreate()
    
    try:
        df = spark.createDataFrame(data)
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        
        print(f"\n📊 Total records: {df.count()}")
        df.select("symbol", "lastPrice", "volume").show(10, truncate=False)
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        final_path = f"{output_path}/date={date_str}"
        
        df.write.mode("overwrite").json(final_path)
        
        print(f"✅ Data saved to: {final_path}")
        print("✨ Collection completed!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://bronze/crypto/binance"
    run(output_path)
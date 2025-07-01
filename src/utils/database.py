import duckdb
import pandas as pd
from pathlib import Path
from decouple import config

class Database:
    def __init__(self):
        self.db_path = config('DB_PATH', default='data/ibama_infracao.db')
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        
    def save_dataframe(self, df: pd.DataFrame, table_name: str):
        """Save dataframe to DuckDB"""
        try:
            # Limpar dados
            df = df.fillna('')
            
            # Converter colunas para string
            for col in df.columns:
                df[col] = df[col].astype(str)
            
            # Salvar
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            self.conn.commit()
            
            print(f"Tabela {table_name} salva com {len(df)} registros")
            return True
            
        except Exception as e:
            print(f"Erro ao salvar: {e}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query"""
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            print(f"Erro na query: {e}")
            return pd.DataFrame()
    
    def get_table_info(self) -> pd.DataFrame:
        """Get table schema information"""
        try:
            return self.conn.execute("PRAGMA table_info(ibama_infracao)").fetchdf()
        except:
            return pd.DataFrame()
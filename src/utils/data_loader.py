import requests
import zipfile
import pandas as pd
import io
from pathlib import Path
from decouple import config

class DataLoader:
    def __init__(self, database=None):
        self.zip_url = config('IBAMA_ZIP_URL')
        self.database = database
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
    def download_and_process(self):
        """Download and process only 2024-2025 data"""
        try:
            print("Baixando dados do IBAMA...")
            
            # Download ZIP
            response = requests.get(self.zip_url, timeout=30)
            if response.status_code != 200:
                print(f"Erro no download: {response.status_code}")
                return False
                
            # Extrair apenas arquivos necessários
            z = zipfile.ZipFile(io.BytesIO(response.content))
            
            target_files = [
                "auto_infracao_ano_2024.csv",
                "auto_infracao_ano_2025.csv"
            ]
            
            all_data = []
            
            for filename in z.namelist():
                if any(target in filename for target in target_files):
                    print(f"Processando {filename}...")
                    
                    # Ler direto do ZIP
                    with z.open(filename) as f:
                        df = pd.read_csv(f, encoding='latin1', sep=';', on_bad_lines='skip')
                        all_data.append(df)
            
            if not all_data:
                print("Nenhum arquivo de 2024/2025 encontrado")
                return False
                
            # Combinar dados
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"Total de registros: {len(combined_df)}")
            
            # Salvar no banco
            if self.database:
                self.database.save_dataframe(combined_df, "ibama_infracao")
                
            return True
            
        except Exception as e:
            print(f"Erro no processamento: {e}")
            return False
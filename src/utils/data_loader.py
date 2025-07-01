import requests
import zipfile
import pandas as pd
import io
from pathlib import Path

# A URL agora será passada pelo script de upload, então o decouple não é mais necessário aqui.

class DataLoader:
    def __init__(self, database=None):
        # A URL será definida externamente
        self.zip_url = None 
        self.database = database
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
    def download_and_process(self):
        """Download and process data from the given zip_url."""
        if not self.zip_url:
            print("Erro: A URL do ZIP não foi definida.")
            return False

        try:
            print("Baixando dados do IBAMA...")
            
            # --- ALTERAÇÃO AQUI: Adicionando verify=False ---
            # Isso instrui o requests a não verificar o certificado SSL do servidor.
            response = requests.get(self.zip_url, timeout=60, verify=False)
            
            # Adiciona um aviso sobre a verificação SSL desabilitada
            if not response.request.verify:
                import urllib3
                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                print("Aviso: A verificação do certificado SSL foi desabilitada para este download.")

            response.raise_for_status() # Lança um erro para status HTTP ruins (4xx ou 5xx)
                
            # Extrair apenas arquivos necessários
            z = zipfile.ZipFile(io.BytesIO(response.content))
            
            # Vamos ser mais flexíveis e pegar qualquer arquivo CSV do ZIP
            csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                print("Nenhum arquivo CSV encontrado no ZIP.")
                return False

            all_data = []
            
            for filename in csv_files:
                print(f"Processando {filename}...")
                try:
                    with z.open(filename) as f:
                        # Tenta ler com UTF-8 primeiro, que é mais moderno. Se falhar, usa latin-1.
                        try:
                            df = pd.read_csv(f, encoding='utf-8', sep=';', on_bad_lines='skip')
                        except UnicodeDecodeError:
                            print(f"  Falha com UTF-8, tentando com latin-1...")
                            # Precisamos reabrir o arquivo, pois ele foi consumido
                            with z.open(filename) as f_latin:
                                df = pd.read_csv(f_latin, encoding='latin1', sep=';', on_bad_lines='skip')
                        all_data.append(df)
                except Exception as e:
                    print(f"  Erro ao processar o arquivo {filename}: {e}")
            
            if not all_data:
                print("Nenhum dado pôde ser extraído dos arquivos CSV.")
                return False
                
            # Combinar dados
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"Total de registros combinados: {len(combined_df)}")
            
            # Salvar no banco
            if self.database:
                self.database.save_dataframe(combined_df, "ibama_infracao")
                
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Erro de rede ao baixar os dados: {e}")
            return False
        except Exception as e:
            print(f"Erro no processamento: {e}")
            return False

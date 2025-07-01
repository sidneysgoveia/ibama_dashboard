import duckdb

conn = duckdb.connect('data/ibama_infracao.db')

# Verificar quantos registros têm UF como NULL ou vazio
print("Verificando valores NULL/vazios em UF...")
result = conn.execute("""
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN UF IS NULL THEN 1 END) as uf_null,
        COUNT(CASE WHEN UF = '' THEN 1 END) as uf_vazio,
        COUNT(CASE WHEN UF = 'N/A' THEN 1 END) as uf_na
    FROM ibama_infracao
""").fetchone()

print(f"Total: {result[0]}")
print(f"UF NULL: {result[1]}")
print(f"UF vazio: {result[2]}")
print(f"UF = 'N/A': {result[3]}")

# Ver distribuição de UFs
print("\nDistribuição de UFs (top 10):")
ufs = conn.execute("""
    SELECT UF, COUNT(*) as total 
    FROM ibama_infracao 
    GROUP BY UF 
    ORDER BY total DESC 
    LIMIT 15
""").fetchdf()
print(ufs)

conn.close()
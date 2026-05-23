import pandas as pd
import numpy as np
import os

# Configuración
NUM_FILAS = 1_000_000
os.makedirs('data', exist_ok=True)
FILE_PATH = 'data/container_data_big.csv'

print(f"🚀 Generando {NUM_FILAS} registros masivos...")

# Datos base
opciones_ptos = ["Chapa GV-PC", "Motores-AS", "Logistica-C1", "Puerto Norte", "Zona Franca"]
opciones_bool = ["SI", "NO"]
opciones_piezas = [
    ("9670847980", "FORRO SUPERIOR GOTERO"),
    ("9670888480", "CIERRE DE GUARDABARRO"),
    ("9800123456", "CONJUNTO MOTOR 1.6"),
    ("9600999888", "OPTICA DELANTERA IZQ"),
    ("9811223344", "PARAGOLPES DELANTERO CRONOS"),
    ("9655443322", "PANEL DE INSTRUMENTOS 208")
]

# Generación de índices aleatorios para las piezas
# Usamos randint para elegir índices del 0 al largo de la lista
indices_piezas = np.random.randint(0, len(opciones_piezas), NUM_FILAS)

# Generación del DataFrame
df = pd.DataFrame({
    "Nro.Contenedor": [f"SUDU{np.random.randint(1000000, 9999999)}" for _ in range(NUM_FILAS)],
    "Pto. Descar": np.random.choice(opciones_ptos, NUM_FILAS),
    "Disponible": np.random.choice(opciones_bool, NUM_FILAS, p=[0.7, 0.3]),
    "En Plant": np.random.choice(opciones_bool, NUM_FILAS, p=[0.2, 0.8]),
    # Extraemos el plano y la descripción usando los índices
    "Plano": [opciones_piezas[i][0] for i in indices_piezas],
    "Desc. Plano": [opciones_piezas[i][1] for i in indices_piezas],
    "Origen": np.random.choice(["CHINA", "EUROPA", "BRASIL", "FRANCIA"], NUM_FILAS),
    "Buque": np.random.choice(["SAN FELIPE", "MSC CATARINA", "MAERSK HANOI", "EVER GIVEN"], NUM_FILAS),
    "Cantidad": np.random.randint(50, 500, NUM_FILAS),
    "Pagado": np.random.choice(opciones_bool, NUM_FILAS, p=[0.8, 0.2])
})

df.to_csv(FILE_PATH, index=False)
print(f"✅ Archivo '{FILE_PATH}' generado con éxito con {NUM_FILAS} filas.")
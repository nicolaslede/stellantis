import os
import time
import pandas as pd
import logging
import sqlite3
from flask import Flask, request, redirect, url_for,  render_template
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Silenciamos los logs genéricos de Python para PySpark
logging.getLogger("pyspark").setLevel(logging.ERROR)

app = Flask(__name__)

# Configuración de Rutas del Entorno Local (Proyecto Stellantis)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "data")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# =====================================================================
# 🛠️ INTEGRACIÓN DE ENTORNO HADOOP LOCAL
# =====================================================================
HADOOP_CUSTOM_PATH = os.path.join(BASE_DIR, "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_CUSTOM_PATH
os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_CUSTOM_PATH, "bin")

# Levantar sesión de Spark optimizada con el Driver JDBC de SQLite
spark = SparkSession.builder \
    .appName("StellantisLogistica") \
    .config("spark.driver.extraClassPath", os.path.join(BASE_DIR, "jars", "sqlite-jdbc-driver.jar")) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# 2. Silenciamos los logs internos de Java una vez que prendió el motor
spark.sparkContext.setLogLevel("ERROR")

@app.route('/')
def login():
    return render_template('login.html')

# 2. El proceso del formulario redirige de forma segura al dashboard
@app.route('/login_proceso', methods=['POST'])
def login_proceso():
    # Captura simulada de datos para el prototipo
    username = request.form.get('username')
    password = request.form.get('password')
    
    # Redirige de manera limpia a la función del dashboard
    return redirect(url_for('index'))

# 3. Tu ruta original de la aplicación (el panel de carga limpio)
@app.route('/dashboard')
def index():
    # Retorna tu index.html original pero con success=False para que espere el Excel
    return render_template('index.html', success=False)


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return render_template('index.html', 
                       success=True, 
                       time=execution_time, 
                       total_patio=total_listos_para_apertura, 
                       contenedores=contenedores_agrupados)
        
    file = request.files['file']
    if file.filename == '':
        return render_template('index.html', error="El nombre del archivo está vacío")

    filename = file.filename
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(file_path)

    # Archivo CSV estandarizado que va a consumir Spark Engine
    spark_csv_path = os.path.join(UPLOAD_FOLDER, "container_data.csv")
    start_time = time.time()

    try:
        # =====================================================================
        # 1. PRE-PROCESADOR: CONVERSIÓN TRANSPARENTE EN TIEMPO REAL
        # =====================================================================
        if filename.endswith('.xlsx') or filename.endswith('.xls'):
            print("📊 Formato Excel detectado. Convirtiendo a CSV intermedio a alta velocidad...")
            with open(file_path, "rb") as f:
                df_excel = pd.read_excel(f, engine='openpyxl')
            df_excel.to_csv(spark_csv_path, index=False, encoding='utf-8')
        else:
            if os.path.exists(spark_csv_path) and file_path != spark_csv_path:
                try:
                    os.remove(spark_csv_path)
                except OSError:
                    pass
            os.rename(file_path, spark_csv_path)

        # =====================================================================
        # 2. SPARK ENGINE: PROCESAMIENTO BIG DATA Y REGLAS DE NEGOCIO
        # =====================================================================
        print("⚡ Spark Engine iniciando procesamiento distribuido...")
        df_raw = spark.read.format("csv").option("header", "true").load(spark_csv_path)

        # Mapeo y Normalización Estricta
        df_normalized = df_raw \
            .withColumnRenamed("Nro. Contenedor", "Nro_Contenedor") \
            .withColumnRenamed("Pto. Descarga", "Pto_Descarga") \
            .withColumnRenamed("Desc. Plano", "Desc_Plano") \
            .withColumnRenamed("En Planta", "En_Planta")

        # Validación estructural de seguridad
        columnas_requeridas = ["Nro_Contenedor", "Pto_Descarga", "Plano", "Pagado", "Disponible", "En_Planta"]
        for col in columnas_requeridas:
            if col not in df_normalized.columns:
                raise ValueError(f"Falta la columna crítica '{col}' en el archivo provisto.")

        # Eliminación de registros nulos en la clave primaria
        df_valido = df_normalized.filter(F.col("Nro_Contenedor").isNotNull())

        # --- FLUJO A: Almacenamiento Histórico Incremental (Formato Parquet) ---
        df_historico = df_valido.withColumn("fecha_ingreso", F.current_timestamp())
        path_parquet = os.path.join(UPLOAD_FOLDER, "historico_analytics")
        df_historico.write.mode("append").parquet(path_parquet)

        # --- FLUJO B: Deduplicación y Filtros Operativos ---
        df_clean = df_valido.dropDuplicates(["Nro_Contenedor", "Plano"])
        
        # Aplicación de Regla de Negocio Administrativa (Si NO está pagado -> NO está disponible)
        df_reglas = df_clean.withColumn(
            "Disponible", 
            F.when(F.col("Pagado") == "NO", "NO").otherwise(F.col("Disponible"))
        )
        
       # FILTRO DE VENTANA OPERATIVA ULTRA ESTRICTO: Los 3 "SI" obligatorios
        df_filtrado = df_reglas.filter(
            (F.col("Pagado") == "SI") & 
            (F.col("Disponible") == "SI") & 
            (F.col("En_Planta") == "SI")
        )


        # =====================================================================
        # MODELADO ENTIDAD-RELACIÓN NORMALIZADO (LAS 3 TABLAS DE LOGÍSTICA)
        # =====================================================================
        
        # TABLA 1: Catálogo Maestro de Piezas
        df_piezas = df_filtrado.select(
            F.col("Plano"), 
            F.col("Desc_Plano").alias("Descripcion_Pieza")
        ).distinct()
        
        # TABLA 2: Maestro de Contenedores Únicos (Datos del contenedor + Score de Prioridad)
        df_contenedores = df_filtrado.select(
            F.col("Nro_Contenedor"), 
            F.col("Pto_Descarga"), 
            F.col("Pagado"), 
            F.col("Disponible"), 
            F.col("En_Planta")
        ).distinct().withColumn(
            "priority_score",
            F.when(F.col("En_Planta") == "SI", 50).otherwise(0) +
            F.when(F.col("Disponible") == "SI", 30).otherwise(0)
        )
        
        # TABLA 3: Stock de Piezas por Contenedor (Relación Uno a Muchos)
        df_stock_piezas = df_filtrado.select(
            F.col("Nro_Contenedor"),
            F.col("Plano"),
            F.col("Cantidad").cast("float").cast("int").alias("Cantidad")
        )

       # =====================================================================
        # 3. CONECTOR DE SALIDA: ESCRITURA EN SQLITE TRANSACTIONAL (BLINDADO)
        # =====================================================================
        print("🔌 Inicializando entorno transaccional SQLite...")
        database_path = os.path.join(BASE_DIR, 'stellantis_data.db')
        
        # PASO PREVENTIVO: Volamos las tablas viejas para limpiar la mugre histórica
        conn_init = sqlite3.connect(database_path)
        cursor_init = conn_init.cursor()
        
        cursor_init.execute("DROP TABLE IF EXISTS maestro_piezas;")
        cursor_init.execute("DROP TABLE IF EXISTS maestro_contenedores;")
        cursor_init.execute("DROP TABLE IF EXISTS stock_piezas_contenedor;")
        
        # Las volvemos a crear estructuralmente limpias y vacías
        cursor_init.execute("CREATE TABLE maestro_piezas (Plano TEXT, Descripcion_Pieza TEXT);")
        cursor_init.execute("CREATE TABLE maestro_contenedores (Nro_Contenedor TEXT, Pto_Descarga TEXT, Pagado TEXT, Disponible TEXT, En_Planta TEXT, priority_score INTEGER);")
        cursor_init.execute("CREATE TABLE stock_piezas_contenedor (Nro_Contenedor TEXT, Plano TEXT, Cantidad INTEGER);")
        
        conn_init.commit()
        conn_init.close()

        db_url = f"jdbc:sqlite:{database_path}"
        db_properties = {
            "driver": "org.sqlite.JDBC",
            "shared_cache": "true",
            "timeout": "30000"  # Aumentamos el timeout a 30 segundos para evitar bloqueos por concurrencia
        }

        print("📊 Sincronizando datos optimizados en el motor de almacenamiento...")
        # Escribimos de forma secuencial controlada para que el driver de SQLite no se sature
        df_piezas.coalesce(1).write.jdbc(url=db_url, table="maestro_piezas", mode="overwrite", properties=db_properties)
        df_contenedores.coalesce(1).write.jdbc(url=db_url, table="maestro_contenedores", mode="overwrite", properties=db_properties)
        df_stock_piezas.coalesce(1).write.jdbc(url=db_url, table="stock_piezas_contenedor", mode="overwrite", properties=db_properties)

        execution_time = round(time.time() - start_time, 2)
        print(f"✅ Proceso completo ejecutado con éxito en {execution_time} segundos.")
       # =====================================================================
        # 4. SMART SEARCH ENGINE: RETRIEVAL OPTIMIZADO PARA LA INTERFAZ HTML
        # =====================================================================
        conn = sqlite3.connect(os.path.join(BASE_DIR, 'stellantis_data.db'))
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                c.Nro_Contenedor, 
                c.En_Planta, 
                c.Disponible, 
                c.priority_score,
                s.Plano, 
                s.Cantidad,
                m.Descripcion_Pieza
            FROM maestro_contenedores c
            JOIN stock_piezas_contenedor s ON c.Nro_Contenedor = s.Nro_Contenedor
            LEFT JOIN maestro_piezas m ON s.Plano = m.Plano
            ORDER BY c.priority_score DESC, c.Nro_Contenedor ASC
        """)
        filas = cursor.fetchall()
        conn.close()

        # Agrupamos los datos estructuradamente (Evita duplicados de contenedores con piezas mixtas)
        contenedores_agrupados = {}
        for fila in filas:
            nc = fila['Nro_Contenedor']
            if nc not in contenedores_agrupados:
                contenedores_agrupados[nc] = {
                    'en_planta': fila['En_Planta'],
                    'disponible': fila['Disponible'],
                    'score': fila['priority_score'],
                    'piezas': []
                }
            contenedores_agrupados[nc]['piezas'].append({
                'plano': fila['Plano'],
                'cantidad': fila['Cantidad'],
                'descripcion': fila['Descripcion_Pieza'] if fila['Descripcion_Pieza'] else "Sin descripción técnica"
            })

      # --- CONTEOS REALES PARA LAS TARJETAS (UNIVERSO EXCLUSIVO) ---
        total_listos_para_apertura = len(contenedores_agrupados)

        # Enviamos los datos consolidados a la interfaz
        return render_template('index.html', 
                               success=True, 
                               time=execution_time, 
                               total_patio=total_listos_para_apertura, # Va directo a la tarjeta principal
                               contenedores=contenedores_agrupados)
        # Enviamos los datos correctos a la interfaz
        return render_template('index.html', 
                               success=True, 
                               time=execution_time, 
                               total_contenedores=total_contenedores_unicos, # <-- Pasamos el conteo real
                               contenedores=contenedores_agrupados)
    except Exception as e:
        print(f"❌ ERROR DETECTADO EN EL PIPELINE: {str(e)}")
        return render_template('index.html', error=str(e))

if __name__ == '__main__':
    app.run(port=5001, debug=True, use_reloader=False)
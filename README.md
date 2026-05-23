# stellantis
Diseño y desarrollo de una plataforma backend integrada con Inteligencia Artificial para la automatización, priorización y optimización del proceso logístico de apertura de contenedores en la planta de producción de Stellantis Argentina


## 🛠️ Requisitos de Instalación y Dependencias


Archivo requerido: sqlite-jdbc.jar

Ubicación: Debe ser descargado y colocado manualmente dentro de la carpeta jar/ en la raíz del proyecto para que el script backendstellantis.py lo detecte dinámicamente al inicializar la SparkSession.
 
Para mantener el repositorio liviano y seguir las buenas prácticas de desarrollo, **no se han incluido** en este repositorio las librerías de Python ni el conector JDBC de la base de datos. 

Antes de ejecutar el sistema por primera vez, asegúrate de cumplir con los siguientes pasos en tu entorno local:

### 1. Entorno de Python (Librerías)
Las dependencias necesarias para la interfaz web (Flask) y el manejo de datos (Pandas/Openpyxl) deben instalarse mediante la terminal. Conéctate a la raíz del proyecto y ejecuta:

```bash
pip install flask pyspark pandas openpyxl



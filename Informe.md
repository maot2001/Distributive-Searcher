# Informe

## Requisitos previos

Antes de comenzar, asegúrate de tener instalados los siguientes paquetes:
- **gensim**==4.1.2  
- **ir_datasets**==0.5.5  
- **numpy**==1.21.5  
- **pypdf**==4.2.0  
- **scikit_learn**==1.4.0  
- **spacy**==3.7.2  
- **streamlit**==1.34.0  
- **joblib**==1.3.2  

### Ejecutar la interfaz del cliente:
Para iniciar la aplicación cliente usando Streamlit, ejecuta el siguiente comando:
```bash
streamlit run client.py
```
### Insertar datos de prueba:
Si deseas insertar información de prueba en el sistema, utiliza:

```bash
python insert.py
```

### Crear un nodo:
Para inicializar un nodo del sistema, ejecuta:

```bash
python __init__.py "t"
```

Donde "t" es el tiempo máximo, en segundos, que tiene el nodo para esperar la respuesta a una consulta realizada por un cliente. 
## Configuración y ejecución usando Docker

Para ejecutar el proyecto en un entorno aislado y reproducible, se recomienda utilizar Docker. En la carpeta del proyecto se encuentra un archivo Dockerfile que contiene los requisitos necesarios para construir la imagen de Docker:

Para crear una imagen de Docker a partir del Dockerfile, utiliza el siguiente comando:

```bash
   docker build -t nombre_imagen .
```
### Pasos para crear un entorno Docker

1. Abre una terminal o consola en el directorio donde tienes los archivos del proyecto.

2. Ejecuta el siguiente comando para crear un contenedor con Docker. Asegúrate de reemplazar "ruta/proyecto/local" por el directorio local donde tienes el código y "ruta/destino/contenedor" por el directorio dentro del contenedor. Además, ajusta el "puerto externo" para exponer el puerto correcto si es necesario.

   ```bash
   docker run -it --rm -v "ruta/proyecto/local":"ruta/destino/contenedor" -p "puerto externo":8501 "imagen_docker" /bin/bash
   ```
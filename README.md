# TP2: Bike Rides Analyzer

## Instrucciones

### Datos:
Los datos a enviar al sistema deben estar almacenados en el directorio `data` en el root del repositorio, con la siguiente estructura:
```
.
├── ...
├── data
│   ├── montreal
│   │   ├── stations.csv
│   │   ├── <trips file>
│   │   └── weather.csv
│   ├── toronto
│   │   ├── stations.csv
│   │   ├── <trips file>
│   │   └── weather.csv
│   ├── washington
│   │   ├── stations.csv
│   │   ├── <trips file>
│   │   └── weather.csv
└── ...
```

El nombre del archivo de trips que se utilizará se configura en `client/config.yaml`, y debe tener el mismo nombre para todas las ciudades.

### Configuración:
En el archivo `config.ini` se especifica la cantidad de instancias que se generarán para cada etapa, así como los parámetros de las tres consultas (precipitaciones mínimas, años, distancia mínima).

### Ejecución:
Antes de iniciar la ejecución es necesario generar el archivo `compose.yaml` corriendo el siguiente script de python:

```
python3 create_docker_compose.py
```

Luego, ejecutar el siguiente comando para levantar los containers e iniciar el sistema en su totalidad:

```
docker compose up --build
```




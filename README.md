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

Para iniciar el sistema, utilizar el script de ejecución como se muestra a continuación:

* ``./tp2.sh build``  Crea las imagenes necesarias.
* ``./tp2.sh run ``   Inicia el sistema.
* ``./tp2.sh help``   Info sobre otros comandos.



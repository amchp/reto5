# Reto 5

## Crear S3 Bucket

- Ir a servicio S3

<img width="1280" alt="Screenshot 2023-05-31 at 10 51 00 AM" src="https://github.com/amchp/reto5/assets/28406146/a259546d-b1d7-4478-adb4-02d003fa8f9f">

- Hacer click en "Create bucket"

<img width="1280" alt="Screenshot 2023-05-31 at 10 50 39 AM" src="https://github.com/amchp/reto5/assets/28406146/53f77f8e-7e33-472f-a876-fa35138214cb">

- Ponerle nombre al bucket y dejar el resto de la configuración por defecto

<img width="1280" alt="Screenshot 2023-05-31 at 10 50 52 AM" src="https://github.com/amchp/reto5/assets/28406146/270a9191-b8c1-44a8-9f9a-cedc27f3156e">

## Crear un cluster EMR

- Primero, entrar AWS y buscar en servicios EMR.

![Screenshot 2023-05-31 at 10 19 49 AM](https://github.com/amchp/reto5/assets/28406146/a8e0b379-7df2-4c54-9b55-591459baa0d0)

- Hacer click en "Create Cluster"

![Screenshot 2023-05-31 at 10 19 54 AM](https://github.com/amchp/reto5/assets/28406146/8860d272-5407-4297-af60-3abc6288da14)

- Ponerle un nombre al cluster
- Escoger la versión de EMR-5.26.0
- Escoger AWS custom
- Escoger:
  - HUE-4.4.0
  - Spark-2.4.3
  - Hadoop-2.8.5
  - Hive-2.3.5
  - Oozie-5.1.0

<img width="1280" alt="Screenshot 2023-05-31 at 10 18 52 AM" src="https://github.com/amchp/reto5/assets/28406146/30976a63-b69c-4ff7-9c0c-56dfe1e891e2">

- Escoger instance group y cambiar las maquinas a m4.large

<img width="1280" alt="Screenshot 2023-05-31 at 10 19 02 AM" src="https://github.com/amchp/reto5/assets/28406146/2ae04376-8e50-40e5-97c2-736f06c57c10">
<img width="1280" alt="Screenshot 2023-05-31 at 10 19 08 AM" src="https://github.com/amchp/reto5/assets/28406146/9eb2eb95-e788-4db6-965d-fcb653ef3c68">

- Escoger el número de máquinas para hacer cada tarea en el cluster
- Esccoger la VPC y subred para el cluster

<img width="1280" alt="Screenshot 2023-05-31 at 10 19 23 AM" src="https://github.com/amchp/reto5/assets/28406146/9ed2d714-3c2f-4bef-b939-b50e09134781">

- Inhabilitar "Use termination protection"

![Screenshot 2023-05-31 at 10 19 28 AM](https://github.com/amchp/reto5/assets/28406146/25b16394-2c12-4c05-8b92-f0d7f264c6cb)

- Escoger una llave para acceder a la máquina (importante para hacer un tunel ssh)

<img width="1280" alt="Screenshot 2023-05-31 at 10 19 33 AM" src="https://github.com/amchp/reto5/assets/28406146/e23ac7ee-5d33-484a-8df7-2d590e1391ba">

- Escoger el rol de servicio y el perfil de las EC2

<img width="1280" alt="Screenshot 2023-05-31 at 10 19 36 AM" src="https://github.com/amchp/reto5/assets/28406146/235b9318-85cd-4846-a0aa-23ea6c2c92df">


## Correr Map/Reduce en el cluster EMR

Hacer un programa en python donde hagas map-reduce con la librería [mrjob](https://mrjob.readthedocs.io/en/stable/index.html)

Crear un archivo de configuración con estas variables:

```
runners:
    emr:
        aws_access_key_id: <ID de llave AWS>
        aws_secret_access_key: <llave secreta AWS>
        aws_session_token: <token de session AWS>
        region: <region AWS>
        ec2_key_pair: <Nombre de llave del cluster>
        ec2_key_pair_file: <Camino a llave del cluster en tu computador>
        ssh_tunnel: true
```

Poner en terminal este commando:
```
python3.11 <camino a programa de python> -r emr <camino a archivo de entrada en s3> \
  --conf-path=<camino a archivo de configuracion> --cluster-id=<id de cluster> \
  --output-dir=<directorio de salida en s3>
```

Ya debería correr tu cluster en AWS y dejar el resultado en el directorio de salida que especificaste en el comando.

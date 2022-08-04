## MUTT DATA Challenge - BITCOIN APP

## Requisitos Previos

* 1 ->  Instalacion de postgresql

## Instalacion del Container

* 1 -> utilizar el comando **_docker build -it bapp ._** para generar el build
 
* 2 -> luego generar  **_docker run --rm -it --network=host bapp**  para correr la configuracion de posgrestql desde el host.

## consideraciones previas
 * 1 - > si se quiere ejecutar toda la configuracion desde el contenedor de postgresql desde docker e instalar todos los paquetes, se puede utilizar el comando **docker exec -it container_id /bin/bash y luego ejecutar pip install paquete**

 * 2 -> para la conexion y creacion de las tablas se utilizo otro usuario y contraseña, se debe colocar en el archivo .env la informacion de accesso correspondiente a bd, usuario y contraseña que en este caso es muttexam, la operacion es transparente

## descripcion de los archivos

* 1 -> **archivo bulk_insertion**: decorador que ejecuta el proceso para transformar y subir la informacion a la base de datos, incluida la respuesta del request (json)

* 2 -> **archivo data_extraction**:  archivo que contiene las funciones que solicitan la informacion, generando un request masivo o solicitando un solo archivo, generando toda la informacion solicitada en archivos json

* 3 -> **archivo tables_db_creation**: archivo que contiene la creacion del la base de datos y la tabla que almacenara la informacion, solicitada a través de los requests.

* 4 -> **archivo main**: archivo principal que ejecuta todas las funciones para que todas las tareas se lleven a cabo

* 5 -> **local_queries**: archivo json que contiene las queries para la limpieza de la informacion en la base de datos, y ejecuta el script de analitica.
       
* 6 -> **archivo .env**: contiene las credenciales de conexión

* 7 -> **Dockerfile**: configuracion para la instalacion del docker

* 8 -> **logging.conf**: archivo de configuracion del log

* 9 -> **requirements**: archivo txt con las librerias necesarias para ejecutar toda la app

## Nota:
* 1 - > **Script de analitica**: el script de analitica lo encontramos como **"q5"  dentro del json y este genera el promedio de los precios en usd de las monedas capturadas en la bd, el script se ejecuta desde la opcion 5 del menu de la app, donde hace el llamado a esta clave del json**.

* 2 -> **los scripts de limpieza y creacion de tablas se encuentran tambien en el archivo json**, la app de python ejecuta la validacion o creacion de tablas desde el inicio del programa y la limpieza de las mismas cuando se insertan datos para no generar datos duplicados.


## Menu de la app:

* 1 -> la app tiene un menu que tiene las siguientes opciones y funcionan de la siguiente forma:
        
    *  **opcion 1** : genera un solo archivo, recibe como parametros fecha y moneda en el siguiente formato: **2022-07-07,bitcoin**
    *  **opcion 2** : genera varios archivos, recibe como parametro solo la fecha en el siguiente formato: **2022-07-07**
    *  **opcion 3** : regresamos al menu principal
    *  **opcion 4** : visualiza los resultados correspondiente a los precios promedios en usd teniendo en cuenta moneda y periodo
    *  **opcion 5** : salida del programa

    ## Nota:
    * **la opcion 1 permite digitar cualquier moneda dentro del espectro de Coingecko, si se digita cualquier otra moneda generara error y no creara el archivo, aun no se ha hecho ninguna validacion**
    
    * **la opcion 2 genera un rango de fechas desde la fecha digitada hasta la fecha actual, esta opcion solo valida las monedas bitcoin, ethereum y cardano** 


## enjoy!

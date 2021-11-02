
# Import data utility

## 1. Crear estructura de archivos.
En el host, debe crear la carpeta `vol` en donde desee. En mi caso lo coloqué en `C:\Users\estudiante\source\repos\PracticasBDD\vol`.

La carpeta `vol` debe tener la siguiente estructura:
```
/vol
	/dumps
		/Parking_Violations_Issued_-_Fiscal_Year_2018.csv
	/node-postgres-csv-data-import
```
* La carpeta `vol/dumps` debe contener los archivos `.csv` fuente.
* La carpeta `vol/node-postgres-csv-data-import` debe contener este repositorio.

## 2. Crear volumen de Docker.
```
docker volume create --driver local --opt type=none --opt device=C:/Users/estudiante/source/repos/PracticasBDD/vol --opt o=bind dockerVol
```

## 3. Crear contenedor de nodo primario con enlace al volumen de Docker recién creado.
```
docker run -it --name ubuntu1 -p 5431:5431 -v dockerVol:/home/vol --tty ubuntu bash
```

## 4. Crear usuario de PostgreSQL.
Esto debe hacerlo en cada uno de los 3 nodos.
```
sudo -u postgres createuser -s -i -d -r -l -w testuser
sudo -u postgres psql -c "ALTER ROLE testuser WITH PASSWORD 'testuser';"
```

## 5. Instalar NodeJS y NPM.
Dentro del contenedor de nodo primario, utilizar los siguientes comandos.
```
sudo apt update
sudo apt install nodejs npm
```

## 6. Instalar dependencias del proyecto.
Dentro del contenedor de nodo primario, colocarse en el directorio raíz de este repositorio.
```
cd /home/vol/node-postgres-csv-data-import
```
Luego, instalar las dependencias.
```
npm install
```

## 7. Crear estructura inicial de la base de datos.
```
npm run create-db
```

## 8. Correr
```
npm start
```
El programa **va a durar mucho tiempo en ejecutarse**. Cada cierto tiempo imprimirá mensajes de progreso.

### Utilidades varias
Puede ignorar esta sección. Únicamente está aquí como referencia.

* Crear volumen de Docker
	```
	docker volume create --driver local --opt type=none --opt device=C:/Users/estudiante/source/repos/PracticasBDD/vol --opt o=bind dockerVol
	```

* Correr contenedor de Docker con configuración de volumen
	```
	docker run -it --name ubuntu1-2 -p 5431:5431 -v dockerVol:/home/vol --tty ubuntu-ddb-2 bash
	```

* Crear usuario de Postgres
	```
	sudo -u postgres createuser -s -i -d -r -l -w testuser
	sudo -u postgres psql -c "ALTER ROLE testuser WITH PASSWORD 'testuser';"
	```

* Conectarse a Postgres por medio de PSQL
	```
	sudo -u postgres psql postgres
	```
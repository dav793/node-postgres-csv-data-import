
# Import data utility

## Instalar dependencias

	```
	npm install
	```

## Correr

	```
	npm run import-data
	```

## Utilidades

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
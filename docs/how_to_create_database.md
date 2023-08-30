# How to create bedbase database

To run bedstat, bedbuncher and bedembed we need to a postgres database.
For development, we can initiate  a local postgres DB using docker.
First, create a persistent volume to house PostgreSQL data:

```bash
docker volume create postgres-data
```

Then, launch the database:
```bash
docker run -d --name bedbase-postgres -p 5432:5432 \
    -e POSTGRES_PASSWORD=bedbasepassword \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_DB=postgres \
    -v postgres-data:/var/lib/postgresql/data postgres:13
```

Now we have created the database and can run pipelines.

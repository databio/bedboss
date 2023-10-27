# How to setup the test environment

## Check if all dependencies are installed or install them

To check if all dependencies are installed run the following command:

go to test folder in the project root directory and run:
```bash
bash ./bash_requirements_test.sh
```

## Set up database or use existing one by changing the config file. Use one of the following options:
1) Open `bedbase_config_test.yaml` file in the `test/test_dependencies` folder, and change database credentials the one that you want to use.
2) Create a new database and user with the credentials that are in the `bedbase_config_test.yaml` file. Credentials that
are in the config file are:
```text
  host: localhost
  port: 5432
  password: docker
  user: postgres
  name: bedbase
```

### To create a test database:

```
docker run --rm -it --name bedbase \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=docker \
  -e POSTGRES_DB=bedbase \
  -p 5432:5432 postgres
```
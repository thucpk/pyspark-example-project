import psycopg2


def get_postgres_cli(postgres_conf):
    postgres_cli = psycopg2.connect(
        database=postgres_conf["database"],
        user=postgres_conf["user"],
        password=postgres_conf["password"],
        host=postgres_conf["host"],
        port=postgres_conf["port"],
    )
    return postgres_cli

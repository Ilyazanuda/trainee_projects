from mysql.connector import connect
from configparser import ConfigParser
from os import walk
import sys


def get_configs() -> tuple:
    """
    Returns parsed configs from config.ini
    """
    file_config = 'config_server.ini'
    config = ConfigParser()
    config.read(file_config)
    config_db, config_parse, config_sql = [list() for _ in range(3)]
    for section in config.sections():
        for parameter_name in config[section]:
            parameter = config[section][parameter_name]
            if section == 'DB':
                config_db.append(int(parameter) if parameter.isdigit() else parameter)
            elif section == 'PARSE':
                config_parse.append(parameter)
            elif section == 'SQL':
                config_sql.append(parameter)

    return config_db, config_parse, config_sql


def execute_query(sql_file, connection, delimiter) -> None:
    """
    Execute received script
    """
    cursor = connection.cursor()
    with open(sql_file, 'r') as file:
        queries = file.read()
        for query in queries.split(delimiter):
            if len(query) == 0:
                continue
            cursor.execute(query)
    connection.commit()


def main() -> None:
    """
    Setup DB and procedure into MySQL server
    """
    config_db, config_parse, config_sql = get_configs()
    hostname, port, username, password = config_db
    delimiter = config_parse[0]

    try:
        with connect(
                     host=hostname,
                     port=port,
                     user=username,
                     passwd=password
                    ) as connection:
            for path in config_sql:
                for (dir_path, _, filenames) in walk(path):
                    for sql_filename in filenames:
                        print(f'Script "{sql_filename}" in progress...', file=sys.stderr)
                        execute_query(dir_path + sql_filename, connection, delimiter)
            connection.close()
        print('Database successfully created.', file=sys.stderr)
    except Exception as error:
        print('Error:', error, file=sys.stderr)


if __name__ == '__main__':
    main()

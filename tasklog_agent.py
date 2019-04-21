#!/usr/bin/python
# -*- coding: utf-8 -*-

import mysql.connector

import datetime
import time

__author__ = "Sartori Davide"
__version__ = "1.0"


def get_conf():
    """ returns the configuration from the given file """
    filename = "tasklog_agent.conf"
    parameters = ["db_username", "db_password", "db_host", "database", "db_table", "log"]
    lists = []
    bools = []
    conf_dict = {}

    for line in open(filename):
        if line.split("=")[0].strip() in parameters:
            conf_dict[line.split("=")[0].strip()] = line.split("=")[1].strip()
        elif line.split("=")[0].strip() in lists:
            parameter_list = []

            for item in line.split("=")[1].strip().split(","):
                parameter_list.append(item.strip())

            conf_dict[line.split("=")[0].strip()] = parameter_list

        elif line.split("=")[0].strip() in bools:
            if line.split("=")[1].strip().lower() == "true":
                conf_dict[line.split("=")[0].strip()] = True
            elif line.split("=")[1].strip().lower() == "false":
                conf_dict[line.split("=")[0].strip()] = False

    return conf_dict


def create_connection():
    """ creates a connection with the database """
    conf_dict = get_conf()

    connection = mysql.connector.connect(user=conf_dict["db_username"], password=conf_dict["db_password"],
                                         host=conf_dict["db_host"], database=conf_dict["database"])

    return connection


def tasklog_query(connection, process, event, level):
    """ executes the query with the given parameters and closes the connection """
    cursor = connection.cursor(buffered=True)

    timestamp = datetime.date.today().strftime('%Y-%m-%d') + " " + time.strftime('%H:%M:%S')

    query = ("INSERT INTO Tasklog(timestamp, process, event, level) VALUES ('" + timestamp + "', '" + process + "', '" +
             event + "', '" + str(level) + "');")

    cursor.execute(query)
    connection.commit()

    connection.close()


if __name__ == "__main__":
    # inizio programma
    database_connection = create_connection()
    # ogni exit()
    tasklog_query(database_connection, "orario", "caricamento completato", 0)

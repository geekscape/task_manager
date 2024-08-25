#!/usr/bin/env python3
#
# Usage: Local DynamoDB set-up
# ============================
# docker-compose up  # Local DynamoDB instance for development testing
# export DB_URL="--endpoint-url http://localhost:8000"
#
# Usage: Task create, list, update, delete commands
# -------------------------------------------------
# ./task_manager --help
# ./task_manager run --sleep 1           # Run TaskManager
# ./task_manager check                   # Test database (developers only)
# ./task_manager create <COMMAND>
# ./task_manager timer  <COMMAND> <CRONTAB>
# ./task_manager list                    # List 50 most recent Tasks
# ./task_manager list --all              # List all Tasks
# ./task_manager list --range start end  # List Tasks within range of ids
# ./task_manager list --verbose          # Show Task fields details
# ./task_manager list --field command <COMMAND>
# ./task_manager list --field id <TASK_ID>
# ./task_manager list --field state <pending|running|success|error>
# ./task_manager update <TASK_ID> <FIELD_NAME> <FIELD_VALUE>
#
# ./task_manager delete <TASK_ID>
# ./task_manager destroy                 # Don't destroy the production database
#
# Usage: AWS CLI
# --------------
# aws dynamodb $DB_URL list-tables
# aws dynamodb $DB_URL describe-table --table-name tasks
# aws dynamodb $DB_URL get-item --table-name tasks --consistent-read \
# aws dynamodb $DB_URL query --table-name tasks \
#     --key-condition-expression "task = :name" \
#     --expression-attribute-values '{":name":{"S":"task"}}'
# aws dynamodb $DB_URL delete-table   --table-name tasks
# aws dynamodb $DB_URL scan --table-name andyg_tasks --output json |  \
#                      jq -r ".Items[]"
#
# Usage: Clean-up and shutdown local DynamoDB
# -------------------------------------------
# ./task_manager.py destroy
# docker-compose down  # Local DynamoDB instance for development testing
#
# Resources
# ~~~~~~~~~
# - https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
# - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/dynamodb.html
# - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#configuring-credentials
# - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
# - https://aws.amazon.com/blogs/database/implement-auto-increment-with-amazon-dynamodb
#
# To Do
# ~~~~~
# * FIX: Improve DynamoDB Tasks table schema partition and sort keys
#   - Partition key: task_type: "task" and "timer"
#   - Each partition maintains "id_next"
#   - Each partition maintains "lowest" and "highest" active "pending" id
#   - Run performance tests before and after improving partition and sort keys
#
# * Create AWS Boto3 configuration data structure
#   * Should be able to configure the AWS data centre region
#   * Include optional local Boto3 endpoint
#
# - FIX: Capture database errors (avoid messy stack traceback)
#   - Database table missing: table.query() and table.scan()
#
# - Review Slack conversation with Louka: trading submission detailed design
#
# - Implement logger with date/time
# - Implement performance measurement
#
# - Refactor "class Database" into separate source code file
# - Create "class Task"
#
# * Implement task_manager import and export commands
#
# - TaskHandler template and example
#   - Use Service Provider Interface (SPI) ?
#   - Process.run()
#   - Update DynamoDB
#   - Run Docker container
#   - Enforce CPU and memory limits
#
# - Design and implement Tasks on DynamoDB ...
#   - Create Task via internal timer Tasks
#   - Create Task from external event (HTTP)
#
# - Web server <--> Web UI
#   - Task queue CRUD
#   - Event --> pending Task

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError as BotocoreClientError
import click
from copy import deepcopy
from datetime import date, datetime, timezone
from flask import Flask
import json
import os
from pprint import pprint
import subprocess
import time
from threading import Thread

from constants import *  #AAG: LEADERBOARD_DB_URL --> DYNAMODB_URL

FORCE_STORE_EPOCH_TIMESTAMP = True  # Store time using seconds since the epoch
RUN_LOOP_SLEEP_TIME = 1  # seconds ... can be overridden on the command line

CONFIGURATION_PATHNAME = ".credentials.json"
# DB_URL = None                   # use AWS hosted DynamoDB instance
DB_URL = "http://localhost:8000"  # use local DynamoDB instance

# During development "DB_PREFIX" avoids everyone stepping on each other's toes
DB_PREFIX = os.environ.get("USER", os.environ.get("USERNAME"))
DB_TASKS_TABLE_NAME = f"tasks"

DB_TABLE_SCHEMAS = {
    DB_TASKS_TABLE_NAME: ("task", "id", 1, 1)
}

# DynamoDB has the "state" reserved word, so "state_" is used instead.
# Task fields "created_at", "run_at" and "stop_at" are when ...
# ... Task creation, invocation start and invocation stop occurred

DB_TASK_FIELD_NAMES = [  # mutable task fields, i.e not task keys
    "state_", "command", "crontab", "diagnostic",
    "created_at", "run_at", "stop_at"
]

# Task state "pending" --> "running" --> ("success" or "error")
# Task state "id_next" (task_id: 0 only) is used by create_unique_item()
# Task state "timer" are persistent Tasks like "cron", which create new Tasks

DB_TASK_STATE_NAMES = [
    "pending", "running", "success", "error", "id_next", "timer"
]

GET_TASKS_RECENT_COUNT = 50

# --------------------------------------------------------------------------- #
# Table: tasks
#   AttributeName: task,       AttributeType: S, KeyType: HASH
#   AttributeName: id,         AttributeType: N, KeyType: RANGE
#   AttributeName: state_,     AttributeType: S, DB_TASK_STATE_NAMES
#   AttributeName: command,    AttributeType: S, Command with parameters
#   AttributeName: diagnostic, AttributeType: S, "Message for success or error"
#   AttributeName: created_at, AttributeType: S, datetime.utcnow().isoformat()
#   AttributeName: run_at,     AttributeType: S, datetime.utcnow().isoformat()
#   AttributeName: stop_at,    AttributeType: S, datetime.utcnow().isoformat()

class Database():
    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self.boto3_session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="us-east-1"  # TODO: Configuration: "ap-southeast-2"
        )

        db_args = {"service_name": "dynamodb"}
        if os.getenv('GBH_ENV') != 'prod':                         #AAG
            db_args["endpoint_url"] = LEADERBOARD_DB_URL           #AAG
    #   self.db_client = self.boto3_session.client(**db_args)      # low-level
        self.db_resource = self.boto3_session.resource(**db_args)  # high-level

        self.create_tables()
        self.tasks_table = self.get_table(DB_TASKS_TABLE_NAME)

# Database tables functions ................................................. #

    def create_table(self, table_name):  #AAG table_schema
        """ # AAG: Restore more succinct data declarative approach
        partition_key_name, sort_key_name,  \
            read_capacity_units, write_capacity_units = table_schema

        key_schema = [{"AttributeName": partition_key_name, "KeyType": "HASH"}]
        if sort_key_name:
            key_schema.append(
                {"AttributeName": sort_key_name, "KeyType": "RANGE"})

        attr_def = [{"AttributeName": partition_key_name, "AttributeType": "S"}]
        if sort_key_name:
            attr_def.append(
                {"AttributeName": sort_key_name, "AttributeType": "N"})
        """

        table = self.db_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    "AttributeName": "task",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "id",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "task",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "id",
                    "AttributeType": "N"    #AAG
                },
                {
                    "AttributeName": "task_type",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "team_id",
                    "AttributeType": "N"
                }
            ],
            BillingMode='PAY_PER_REQUEST',
            GlobalSecondaryIndexes=[{
                'IndexName': 'task_type_index',
                'KeySchema': [
                    {
                        'AttributeName': 'task_type',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'team_id',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
            }
            ]
        )

        table.wait_until_exists()
        return table

    def create_tables(self):
        for table_name, _ in DB_TABLE_SCHEMAS.items():
            table = self.get_table(table_name)
            if not table:
                table = self.create_table(table_name)

    """
    def describe_table(self, table_name):
        table_description = self.db_client.describe_table(TableName=table_name)
        pprint(table_description)
    """

    def destroy_tables(self):
        for table_name in DB_TABLE_SCHEMAS.keys():
            table = self.get_table(table_name)
            if table:
                table.delete()
                table.wait_until_not_exists()

    def get_table(self, table_name):
        table = None
        if table_name in self.get_table_names():
            table = self.db_resource.Table(table_name)
        return table

    def get_table_attributes(self, table_name):
        table = self.get_table(table_name)
        return table.attribute_definitions

    def get_table_data(self, table_name):
        table = self.get_table(table_name)
        return table.scan()

    def get_table_names(self):
    #   table_names = self.db_client.list_tables()["TableNames"]
        table_names = [table.name for table in self.db_resource.tables.all()]
        return table_names

# Database tasks functions .................................................. #

    def create_task(self, command, custom_fields=None):
        fields = {
            "state_": "pending", "command": command, "diagnostic": "",
            "created_at": self.datetime_now_utc_iso(),
            "run_at": "", "stop_at": ""
        }
        if custom_fields:
            fields.update(custom_fields)
        return self.create_unique_item(self.tasks_table, "task", fields)

    def create_timer(self, command, crontab):
        fields = {
            "state_": "timer", "command": command, "diagnostic": "",
            "crontab": crontab,
            "created_at": self.datetime_now_utc_iso(),
            "run_at": self.datetime_epoch()[1], "stop_at": ""
        }
        return self.create_unique_item(self.tasks_table, "task", fields)

# TODO: Check "result['Count']" correctness, now that "task_id" is an integer
# TODO: Check that update condition f"attribute_not_exists({id_value})" works ?
# See https://dynobase.dev/dynamodb-python-with-boto3/#increment-item-attribute

    def create_unique_item(self, table, item_type, fields=None):
        id_key = id_value = item_type
        id_next = self.get_id_next(table, item_type)

        replace_task_id = False
        if fields["state_"] == "pending":
            replace_task_id = "<TASK_ID>" in fields["command"]

        saved = False
        while not saved:  # put new item, but only if the item doesn't exist
            try:
                id = id_next
                item = {id_key: id_value, "id": id}
                if fields:
                    item.update(fields)
                    if replace_task_id:
                        item["command"] = fields["command"].replace(
                                                "<TASK_ID>", str(id))
                item = self.normalize_out(item)
                condition_expr = f"attribute_not_exists({id_value})"
                table.put_item(Item=item, ConditionExpression=condition_expr)
                saved = True
        #   except dynamo.meta.table.exceptions.ConditionalCheckFailedException
            except Exception as exception:   # handle race condition, try again
                id_next += 1

        self.update_task(0, {"id_next": id_next + 1})
        return id

    def delete_task(self, task_id):
        self.tasks_table.delete_item(
            Key={"task": "task", "id": int(task_id)})

    def get_id_next(self, table=None, item_type="task"):
        if not table:
            table=self.tasks_table
        result = table.get_item(Key={"task": "task", "id": 0})
        if "Item" not in result:
            id_key = id_value = item_type
            id_next = 1
            item={
                id_key: id_value, "id": 0, "state_": "id_next",
                "id_next": id_next, "command": "", "diagnostic": "",
                "created_at": self.datetime_now_utc_iso(),
                "run_at": "", "stop_at": ""
            }
            table.put_item(Item=self.normalize_out(item))
        else:
            id_next = int(result["Item"]["id_next"])
        return id_next

    def get_task(self, task_id):
        key = {"task": "task", "id": int(task_id)}
        result = self.tasks_table.get_item(Key=key)
        task = None
        if "Item" in result:
            task = self.normalize_in(result["Item"])
        return task

# tasks = get_tasks(task_id, filter, sort_field)
#     task_id = 1
#     filter = ("state_", "pending")  # ("key", "value")
#     range = (id_start, id_end)      # all: (1, -1)
#     sort_field = "id"

    def get_tasks(self, task_id=None, filter=None, range=None, sort_field=None):
        if not task_id:
            id_end = max(1, self.get_id_next() - 1)
            if not range:
                id_start = max(1, id_end - GET_TASKS_RECENT_COUNT + 1)
            else:
                id_start = max(1, range[0])
                id_end = id_end if range[1] == -1 else min(range[1], id_end)

            key_condition_expr =  \
                Key("task").eq("task") & Key("id").between(id_start, id_end)
        else:
            task_id = int(task_id)
            key_condition_expr = Key("task").eq("task") & Key("id").eq(task_id)

        query_arguments = {"KeyConditionExpression": key_condition_expr}

        if filter:
            query_arguments["FilterExpression"] = Attr(filter[0]).eq(filter[1])

        tasks = []
        while True:
            results = self.tasks_table.query(**query_arguments)
            tasks.extend(results["Items"])
            if "LastEvaluatedKey" not in results:
                break
            query_arguments["ExclusiveStartKey"] = results["LastEvaluatedKey"]

        if sort_field:
            key_default = ""
            if sort_field in "id":
                key_default = 0
            get_key_function = lambda d: d.get(sort_field, key_default)
            tasks = sorted(tasks, key=get_key_function)

        for task in tasks:
            task = self.normalize_in(task)
        return tasks

    def normalize_in(self, item):  # Validate / convert incoming Task fields
        if "id" in item:
            item["id"] = int(item["id"])

        if FORCE_STORE_EPOCH_TIMESTAMP:
            for key in ["created_at", "run_at", "stop_at"]:
                if key in item:
                    epoch_seconds = int(item[key])
                    item[key] = self.epoch_to_utc_iso(int(item[key]))
        return item

    def normalize_out(self, item):  # Validate / convert out-going Task fields
        if FORCE_STORE_EPOCH_TIMESTAMP:
            item = deepcopy(item)  # don't trash internal field representation
            for key in ["created_at", "run_at", "stop_at"]:
                if key in item:
                    utc_iso_value = item[key]
                    if len(item[key]):
                        item[key] = int(self.utc_iso_since_epoch(item[key]))
                    else:
                        item[key] = 0
        return item

    def print_tasks(self, tasks, field_names=None, prefix="  Task"):
        if field_names:
            if field_names != "verbose":
                tasks = self.filter_dicts_by_keys(tasks, field_names)
        for task in tasks:
            if not field_names:
                command = task["command"]
                created_at = self.utc_iso_to_local(task["created_at"])
                id = int(task["id"])
                state_ = task["state_"]
                if state_ == "timer":
                    command = f"[{task['crontab']}] {command}"
                print(f"{prefix} {id:06d}: {created_at} {state_:7}: {command}")
            else:
                pprint(task)

    def update_task(self, task_id, fields):
        key = {"task": "task", "id": int(task_id)}
        expression_attr_values = {}
        fields = self.normalize_out(fields)

        index = 0
        update_expr = "SET"
        for field_name, field_value in fields.items():
            if index > 0:
                update_expr += ","
            variable_name = f"var{index}"
            update_expr += f" {field_name} = :{variable_name}"
            expression_attr_values[f":{variable_name}"] = field_value
            index += 1

        self.tasks_table.update_item(
            Key=key,
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expression_attr_values
        )

    def valid_task_field_name(self, field_name):
        return field_name in DB_TASK_FIELD_NAMES

# Database miscellaneous functions .......................................... #

    def datetime_epoch(self):
        epoch = "1970-01-01T00:00:00.000000"
        return datetime(1970, 1, 1), epoch

    def datetime_now_utc_iso(self):
        return datetime.utcnow().isoformat()

    def epoch_to_utc_iso(self, seconds_since_epoch):
        return datetime.utcfromtimestamp(seconds_since_epoch).isoformat()

    def local_iso_now(self):
      return self.utc_iso_to_local(self.datetime_now_utc_iso())

    def utc_iso_since_epoch(self, datetime_utc_iso):
        datetime_utc = self.utc_iso_to_datetime(datetime_utc_iso)
        return (datetime_utc - self.datetime_epoch()[0]).total_seconds()

    def utc_iso_to_datetime(self, datetime_utc_iso):
    #   datetime_utc = date.fromisoformat(datetime_utc_iso)  # should work :(
        if len(datetime_utc_iso) == 19:
            strp_isoformat = "%Y-%m-%dT%H:%M:%S"
        else:
            strp_isoformat = "%Y-%m-%dT%H:%M:%S.%f"
        datetime_utc = datetime.strptime(datetime_utc_iso, strp_isoformat)
        return datetime_utc

    def utc_iso_to_local(self, datetime_utc_iso):
        datetime_utc = self.utc_iso_to_datetime(datetime_utc_iso)
        datetime_local = datetime_utc.replace(
            tzinfo=timezone.utc).astimezone(tz=None)
        return datetime_local.isoformat().replace("T", " ")[:19]

    # Filter a list of keys and values from a list of dictionaries
    #     dicts: [{"a": 1, "b:2"}], keys: ["a"] --> [{"a": 1}]

    def filter_dicts_by_keys(self, dicts, keys):
        results = [{key: d[key] for key in keys if key in d} for d in dicts]
        return results

# --------------------------------------------------------------------------- #

class TaskManager():
    def __init__(self, database):
        self.database: Database = database

    def process_start(self, task, new_state):
        task_id = int(task["id"])
        command = task["command"]
        run_previous = task["run_at"]
        task["state_"] = new_state
        update_arguments = {"state_": task["state_"]}
        if task["diagnostic"] != "":
            task["diagnostic"] = ""
            update_arguments["diagnostic"] = task["diagnostic"]
        if new_state != "timer":
            task["run_at"] = self.database.datetime_now_utc_iso()
            update_arguments["run_at"] = task["run_at"]
        self.database.update_task(task_id, update_arguments)
        return task_id, command, run_previous

    def process_task(self, task):
        task_id, command, run_previous = self.process_start(task, "running")
        tokens = command.split()
        if tokens[0] == "echo":
            print(f"---- {' '.join(tokens[1:])}")
        elif tokens[0] == "sleep":
            time.sleep(int(tokens[1]))
        else:
            try:
                result = subprocess.run(tokens, check=True, shell=False)
            except subprocess.CalledProcessError as called_process_error:
                task["state_"] = "error"
                error_code = called_process_error.returncode
                task["diagnostic"] = f"Error code {error_code}: {command}"
            except Exception as exception:
                task["state_"] = "error"
                task["diagnostic"] = f"Error: {exception}"

        if task["diagnostic"]:
                print(f"BOOM {task['diagnostic']}")

        task["stop_at"] = self.database.datetime_now_utc_iso()
        update_arguments = {"stop_at": task["stop_at"]}
        if task["state_"] == "error":
            update_arguments["diagnostic"] = task["diagnostic"]
        else:
            task["state_"] = "success"
        update_arguments["state_"] = task["state_"]
        self.database.update_task(task_id, update_arguments)

    def crontab_error(self, task):
        task_id = int(task["id"])
        diagnostic = f"Error: Invalid crontab field: {task['crontab']}"
        print(f"Timer {task_id:06d}: {diagnostic}")
        self.database.update_task(task_id, {"diagnostic": task["diagnostic"]})
        task["state_"] = "error"
        self.database.update_task(task_id, {"state_": task["state_"]})

    def process_timer(self, task):
        task_id, command, run_previous = self.process_start(task, "timer")
        crontabs = task["crontab"].split()
        if len(crontabs) != 6:
            self.crontab_error(task)
        else:
            time_scales = [1, 60, 3600, 86400]  # seconds
            for time_index in range(len(time_scales)):
                time_scale = time_scales[time_index]
                crontab = crontabs[time_index]
                if "/" in crontab:
                    try:
                        absolute_time, relative_time = crontab.split("/")
                    except ValueError as value_error:
                        self.crontab_error(task)

                    run_previous = self.database.utc_iso_since_epoch(
                        run_previous)
                    run_previous_delta = time.time() - run_previous
                    if run_previous_delta >= int(relative_time) * time_scale:
                        new_task_id = self.database.create_task(command)
                        task["run_at"] = self.database.datetime_now_utc_iso()
                        self.database.update_task(
                            task_id, {"run_at": task["run_at"]})
                        new_task = self.database.get_task(new_task_id)
                        self.database.print_tasks(
                            [new_task], prefix="++++ Create")

    def run(self, run_loop_sleep_time=RUN_LOOP_SLEEP_TIME, once=False):
        if not once:
            Thread(target=self.run_timers, args=(run_loop_sleep_time, )).start()

        first_loop = True
        loop_start_time = time.time()

        while True:
            tasks_pending_processed = 0
            tasks_start_time = time.time()

            get_tasks_arguments = {
                "filter": ("state_", "pending"), "sort_field": "id" }

            if first_loop:
                first_loop = False
                get_tasks_arguments["range"] = (1, -1)  # all
            else:
                id_recent = max(1, self.database.get_id_next() - 500)
                get_tasks_arguments["range"] = (id_recent, -1)

            tasks = self.database.get_tasks(**get_tasks_arguments)
        #   print(f"#### Fetched {task_state} Tasks: count={len(tasks)}")

            for task in tasks:
                task_start_time = time.time()
                now = self.database.local_iso_now()
                prefix = f"vvvv {now} Running Task"
                self.database.print_tasks([task], prefix=prefix)

                self.process_task(task)

                task_process_time = time.time() - task_start_time
                now = self.database.local_iso_now()
                prefix = f"^^^^ {now} Stopped Task {task['id']:06d}: "
                print(f"{prefix}{task_process_time:.3f} seconds")
                tasks_pending_processed += 1

            if tasks_pending_processed:
                tasks_process_time = time.time() - tasks_start_time
                print(f"#### Tasks processed: {tasks_pending_processed}: "
                      f"{tasks_process_time:.3f} seconds ####")
            else:
                loop_process_time = time.time() - loop_start_time
                sleep_time = max(0, run_loop_sleep_time - loop_process_time)
                if sleep_time > 0.0:
                #   if sleep_time >= 0.2:
                #       print(f"#### Sleeping: {sleep_time:.3f} seconds ####\n")
                    time.sleep(sleep_time)
                loop_start_time = time.time()

            if once:
                break

    def run_timers(self, run_loop_sleep_time=RUN_LOOP_SLEEP_TIME):
        while True:
            loop_start_time = time.time()

            timers = self.database.get_tasks(  # TODO: Too slow at scale
                filter=("state_", "timer"), range=(1, -1), sort_field="id")

            for timer in timers:
                self.process_timer(timer)

            loop_process_time = time.time() - loop_start_time
            sleep_time = max(0, run_loop_sleep_time - loop_process_time)
            if sleep_time > 0.0:
                time.sleep(sleep_time)

# --------------------------------------------------------------------------- #

def check_database(database):
    table_names = database.get_table_names()
    print(f"#### DB tables: {table_names}")

    print("\n-------------------------------")
    for table_name in table_names:
        print(f"#### DB Table: {table_name}")
    #   database.describe_table(table_name)  # low-level
        table_attributes = database.get_table_attributes(table_name)
        print(f"DB table attributes: {table_attributes}")
        print()

        table_data = database.get_table_data(table_name)
        table_item_count = table_data["Count"]
        table_items = table_data["Items"]
        print(f"DB table item count: {table_item_count}")
        if table_item_count == 1:
            print(f"DB table items: {table_items[0]}")
        if table_item_count > 1:
            print(f"DB table items: {table_items[0].keys()}")
        print("\n-------------------------------")

    task_id = database.create_task("echo 0")  # create: 1
    task_id = database.create_task("echo 2")  # create: 2
    task_id = database.create_task("echo 3")  # create: 3
    database.delete_task(task_id - 1)         # delete: 2

    print(f"DB Task 1")
    task = database.get_task("1")
    if task:
        database.print_tasks([task])
    print(f"DB Tasks")
    database.print_tasks(database.get_tasks(sort_field="id"))

    print("\n-------------------------------")
    database.update_task("1", {"command": "echo 1"})
    database.print_tasks([database.get_task("1")])

def load_configuration(configuration_file):
    try:
        with open(configuration_file) as file:
            configuration = json.load(file)
            aws_access_key_id = configuration["AWS_ACCESS_KEY_ID"]
            aws_secret_access_key = configuration["AWS_SECRET_ACCESS_KEY"]
    except FileNotFoundError as file_not_found_error:
        raise SystemExit(str(file_not_found_error))
    except KeyError as key_error:
        raise SystemExit(
            f"Configuration: '{configuration_file}' requires {key_error}")
    return aws_access_key_id, aws_secret_access_key

def validate_field_name(database, field_name):
    if not database.valid_task_field_name(field_name):
        diagnostic = f"Error: Invalid field name: {field_name}\n"  \
                     f"Valid field names: {' '.join(DB_TASK_FIELD_NAMES)}"
        raise SystemExit(diagnostic)

@click.group("main")
@click.pass_context
def main(context):
    """Task Manager server"""
    aws_access_key_id, aws_secret_access_key = load_configuration(
        CONFIGURATION_PATHNAME)
    database = Database(aws_access_key_id, aws_secret_access_key)
    context.obj = TaskManager(database)

@main.command(help="Check (test) database")
@click.pass_obj
def check(task_manager):
    check_database(task_manager.database)

@main.command(name="create", help="Create task")
@click.pass_obj
@click.argument("command", nargs=1, required=True, default=None)
def create_task(task_manager, command):
    database = task_manager.database
    task_id = database.create_task(command)
    task = database.get_task(task_id)
    database.print_tasks([task])

@main.command(name="timer")
@click.pass_obj
@click.argument("command", nargs=1, required=True, default=None)
@click.argument("crontab", nargs=1, required=True, default=None)
def create_timer(task_manager, command, crontab):
    """CRONTAB field ... inspired by, but not exactly the same as Unix crontab

    \b
    .------------------------ second       (0 - 59)
    |   .-------------------- minute       (0 - 59)
    |   |   .---------------- hour         (0 - 23)
    |   |   |   .------------ day of month (1 - 31)
    |   |   |   |   .-------- month        (1 - 12)
    |   |   |   |   |   .---- day of week  (1 - 7): Monday = 1
    |   |   |   |   |   |
    *   *   *   *   *   *     "*" is a wildcard, i.e matches any value.

    \b
    0   0   0   *   *   *     "n" matches exact value, e.g just at midnight
    0   *   0/4 *   *   *     "Absolute time/Relative time", e.g every 4 hours
    *   0/1 *   *   *   *     "Absolute time/Relative time", e.g every minute
    """
    database = task_manager.database
    timer_id = database.create_timer(command, crontab)
    timer = database.get_task(timer_id)
    database.print_tasks([timer])

@main.command(name="delete", help="Delete task")
@click.pass_obj
@click.argument("task_id", type=int, nargs=1, required=True, default=None)
def delete_task(task_manager, task_id):
    task_manager.database.delete_task(task_id)

@main.command(help="Destroy database tables")
@click.pass_obj
def destroy(task_manager):
    confirm = "YES"
    if os.getenv('GBH_ENV') == 'prod':  # using AWS DynamoDB production database ?
        confirm = input('To destroy the production database, type "YES": ')
    if confirm == "YES":
        task_manager.database.destroy_tables()
        print("Database table(s) deleted")

@main.command(name="list",
    help="List tasks optionally searching by field, e.g command, id, state")
@click.pass_obj
@click.option("--all", "-a", is_flag=True, help="List all Tasks")
@click.option("--field", "-f", nargs=2, help="List Tasks with this field value")
@click.option("--range", "-r", nargs=2, help="List Tasks within range of ids")
@click.option("--verbose", "-v", is_flag=True, help="Show Task fields details")
def list_tasks(task_manager, all, field, range, verbose):
    database = task_manager.database
    filter = None
    tasks = None
    if field:
        field_name, field_value = field
        if field_name == "id":
            task_id = int(field_value)
            task = database.get_task(task_id)
            if not task:
                raise SystemExit(f"Error: Task id {task_id:06d} not found")
            tasks = [task]
        else:
            if field_name == "state":
                field_name = "state_"
            validate_field_name(database, field_name)
            filter = (field_name, field_value)
    if range:
        try:
            range = (int(range[0]), int(range[1]))
        except ValueError:
            raise SystemExit("Error: Task list range must be two integers")
        if range[0] > range[1]:
            raise SystemExit('Error: Task list range "start" larger than "end"')
    if not tasks:
        range = (1, -1) if all else range
        tasks = database.get_tasks(filter=filter, range=range, sort_field="id")
    database.print_tasks(tasks, "verbose" if verbose else None)

@main.command(help="Run task_manager server")
@click.pass_obj
@click.option("--sleep", "-s", nargs=1, type=int, default=RUN_LOOP_SLEEP_TIME,
    help="Run loop sleep time")
def run(task_manager, sleep):
    task_manager.run(run_loop_sleep_time=sleep)

@main.command(name="update", help="Update task field, e.g command, state")
@click.pass_obj
@click.argument("task_id", type=int, nargs=1, required=True, default=None)
@click.argument("field_name", nargs=1, required=True, default=None)
@click.argument("field_value", nargs=1, required=True, default=None)
def update_task(task_manager, task_id, field_name, field_value):
    database = task_manager.database
    if field_name == "state":
        field_name = "state_"
    validate_field_name(database, field_name)
    database.update_task(task_id, {field_name: field_value})
    task = database.get_task(task_id)
    database.print_tasks([task])

if __name__ == "__main__":
    main()

# --------------------------------------------------------------------------- #

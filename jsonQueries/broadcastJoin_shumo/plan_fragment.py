#!/usr/bin/env python

import json

def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

def connection_info(host):
    connection_info = {
        "driverClass": "com.vertica.jdbc.Driver",
        "dbms": "vertica",
        "host": host,
        "port": "15433",
        "username": "dbadmin",
        "database": "mrbenchmarks",
        "password": "mrbenchmarks"
    }

    return connection_info

def scan_broadcast(relation_key):
    
    scan = {
        "opName": "ScanR",
        "opType": "TableScan",
        "relationKey": relation_key,
        "connectionInfo": connection_info("localhost")
    }
    
    broadcast = {
        "argChild": "ScanR",
        "argOperatorId": "broadcast",
        "opName": "broadcast",
        "opType": "BroadcastProducer"
    }

    fragment = {
        "operators": [ scan, broadcast ]
    }

    return fragment

def scan_join(relation_key):
    scan = {
        "opName": "ScanU",
        "opType": "TableScan",
        "relationKey": {
            "programName": "broadcastjoin",
            "relationName": "UserBase",
            "userName": "shumochu"
        },
        "connectionInfo":connection_info("localhost")
    }
    broadcast_consumer = {
        "argOperatorId": "broadcast",
        "arg_schema": {
            "columnTypes" : ["LONG_TYPE","STRING_TYPE","LONG_TYPE"],
            "columnNames" : ["pageRank","pageURL","avgDuration"]
        },
        "opName": "receive",
        "opType": "BroadcastConsumer"
    }
    join = {
        "argChild1": "receive",
        "argChild2": "ScanU",
        "arg_columns1": [ 1 ],
        "arg_columns2": [ 1 ],
        "arg_select1": [ 0 ],
        "arg_select2": [ 1 ],                
        "opName": "Join",
        "opType": "SymmetricHashJoin"
    }
    insert = {
        "argChild": "Join",
        "opName": "Insert",
        "opType": "DbInsert",
        "relationKey": relation_key,
        "argOverwriteTable": True,
        "connectionInfo": ("localhost")
    }
    fragment = {
        "operators": [ scan, broadcast_consumer, join, insert ]
    }

    return fragment

def generate_broadcastjoin():
    
    table_R_key = {
        "programName": "broadcastjoin",
        "relationName": "RankBase2x",
        "userName": "shumochu"
    }
    result_key = {
        "programName": "broadcastjoin",
        "relationName": "result2x",
        "userName": "shumochu"
    }
    
    fragments = [ scan_broadcast(table_R_key), scan_join(result_key) ]

    whole_plan = {
        "fragments": fragments,
        "logicalRa": "broadcast join",
        "rawDatalog": "broadcast join"
    }

    return whole_plan

def scan_and_shuffle(relation_key, relation_name):
    scan = {
        "opName": "Scan("+relation_name+")",
        "opType": "TableScan",
        "relationKey": relation_key,
        "connectionInfo": connection_info("localhost")
    }
    shuffle = {
        "argChild": "Scan("+relation_name+")",
        "argOperatorId": "Shuffle("+relation_name+")",
        "opName": "Shuffle("+relation_name+")",
        "opType": "ShuffleProducer",
        "argPf": {
            "index": 1,
            "type": "SingleFieldHash"
        }
    }
    fragment = {
        "operators": [scan, shuffle ]       
    }

    return fragment

def receive_and_join(relation_key):
    gatherR = {
        "argOperatorId": "Shuffle(R)",
        "arg_schema": {
            "columnNames": [
                "pageRank","pageURL","avgDuration"
            ],
            "columnTypes": [
                "LONG_TYPE","STRING_TYPE","LONG_TYPE"
            ]
        },
        "opName": "Gather(R)",
        "opType": "ShuffleConsumer"
    }

    getherS = {
        "argOperatorId": "Shuffle(S)",
        "arg_schema": {
            "columnNames": [
                "sourceIPAddr","destinationURL","visitDate","adRevenue","UserAgent","cCode","lCode","sKeyword", "avgTimeOnSite"
            ],
            "columnTypes": [
                "STRING_TYPE","STRING_TYPE","DATETIME_TYPE","FLOAT_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","LONG_TYPE"
            ]
        },
        "opName": "Gather(S)",
        "opType": "ShuffleConsumer"
    }

    join = {
        "argChild1": "Gather(R)",
            "argChild2": "Gather(S)",
            "arg_columns1": [
                1
            ],
            "arg_columns2": [
                1
            ],
            "arg_select1": [
                0
            ],
            "arg_select2": [
                1
            ],
            "opName": "Join",
            "opType": "SymmetricHashJoin"
    }
    insert = {
        "argChild": "Join",
        "opName": "InsertResult",
        "opType": "DbInsert",
        "relationKey": relation_key,
        "argOverwriteTable": True,
        "connectionInfo": connection_info("localhost")
    }
    fragments = {
        "operators": [gatherR, getherS, join, insert]
    }

    return fragments

def generate_partition_join():
    
    table_R_key = {
        "programName": "broadcastjoin",
        "relationName": "RankBase2x",
        "userName": "shumochu"
    }
    table_S_key = {
        "programName": "broadcastjoin",
        "relationName": "UserBase",
        "userName": "shumochu"
    }
    result_key = {
        "programName": "broadcastjoin",
        "relationName": "PartitonJoinResult2x",
        "userName": "shumochu"
    }

    fragments = [ scan_and_shuffle(table_R_key,"R"),
                  scan_and_shuffle(table_S_key,"S"),
                  receive_and_join(result_key)
                ]

    whole_plan = {
        "fragments": fragments,
        "logicalRa": "partiton join",
        "rawDatalog": "partition join"
    }
    return whole_plan

def scan_collect(relation_key):
    scan = {
        "opName": "ScanR",
        "opType": "TableScan",
        "relationKey": relation_key,
        "connectionInfo": connection_info("localhost")
    }
    collect_producer = {
        "argChild": "ScanR",
        "argOperatorId": "Collect",
        "opName": "CollectProducer",
        "opType": "CollectProducer"
    }
    fragment = {
        "operators": [scan, collect_producer]
    }
    return fragment

def collect_insert(relation_key, schema, worker_id):
    collect_consumer = {
        "argOperatorId": "Collect",
        "arg_schema": schema,
        "opName": "Gather",
        "opType": "CollectConsumer"
    }
    insert = {
        "argChild": "Gather",
        "opName": "Insert",
        "opType": "DbInsert",
        "relationKey": relation_key,
        "argOverwriteTable": True,
        "connectionInfo": connection_info("localhost")
    }
    fragment = {
        "operators": [collect_consumer, insert],
        "workers": [ worker_id ]
    }
    return fragment

def concatenate():
    
    rank_schema = {
        "columnNames": [ "pageRank","pageURL","avgDuration" ],
        "columnTypes": [ "LONG_TYPE","STRING_TYPE","LONG_TYPE" ]
    }
    user_schema = {
        "columnNames": [
            "sourceIPAddr","destinationURL","visitDate","adRevenue","UserAgent","cCode","lCode","sKeyword", "avgTimeOnSite"
        ],
        "columnTypes": [
            "STRING_TYPE","STRING_TYPE","DATETIME_TYPE","FLOAT_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","STRING_TYPE","LONG_TYPE"
        ]
    }
    rank_table_key = {
        "programName": "broadcastjoin",
        "relationName": "RankBase2x",
        "userName": "shumochu"
    }
    user_table_key = {
        "programName": "broadcastjoin",
        "relationName": "UserBase",
        "userName": "shumochu"
    }
    rank_table_insert_key = {
        "programName": "broadcastjoin",
        "relationName": "RankBase2xUnion8workers",
        "userName": "shumochu"
    }
    user_table_insert_key = {
        "programName": "broadcastjoin",
        "relationName": "UserBaseUnion8workers",
        "userName": "shumochu"
    }

    fragments = [ scan_collect(rank_table_key), collect_insert(rank_table_insert_key, rank_schema, 1)]

    whole_plan = {
        "fragments": fragments,
        "logicalRa": "union table in 8 workers",
        "rawDatalog": "union table in 8 workers"
    }
    return whole_plan


#generate broadcast join plan
#print pretty_json(generate_broadcastjoin())

#generate partition join plan
#print pretty_json(generate_partition_join())

#concatenate the result table plan
print pretty_json(concatenate())

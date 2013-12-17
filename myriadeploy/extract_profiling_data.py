#!/usr/bin/env python

from collections import defaultdict
import re
import sys
import json
import copy
import myriadeploy
import cPickle as pickle
import os.path

# root operators
root_operators = set(['LocalMultiwayProducer',
                      'CollectProducer',
                      'RecoverProducer',
                      'ShuffleProducer',
                      'BroadcastProducer',
                      'SinkRoot',
                      'DbInsert',
                      'EOSController'
                      ])

# By default, all operators have no children
children = defaultdict(list)
# Populate the list for all operators that do have children
children['CollectProducer'] = ['arg_child']
children['RecoverProducer'] = ['arg_child']
children['EOSController'] = ['arg_child']
children['IDBInput'] = [
    'arg_initial_input', 'arg_iteration_input', 'arg_eos_controller_input']
children['RightHashJoin'] = ['arg_child1', 'arg_child2']
children['RightHashCountingJoin'] = ['arg_child1', 'arg_child2']
children['SymmetricHashJoin'] = ['arg_child1', 'arg_child2']
children['LocalMultiwayProducer'] = ['arg_child']
children['MultiGroupByAggregate'] = ['arg_child']
children['SingleGroupByAggregate'] = ['arg_child']
children['ShuffleProducer'] = ['arg_child']
children['DbInsert'] = ['arg_child']
children['Aggregate'] = ['arg_child']
children['Apply'] = ['arg_child']
children['Filter'] = ['arg_child']
children['UnionAll'] = ['arg_children']
children['Merge'] = ['arg_children']
children['ColumnSelect'] = ['arg_child']
children['SymmetricHashCountingJoin'] = ['arg_child1', 'arg_child2']
children['BroadcastProducer'] = ['arg_child']
children['HyperShuffleProducer'] = ['arg_child']
children['SinkRoot'] = ['arg_child']
children['DupElim'] = ['arg_child']
children['Rename'] = ['arg_child']

million = int(1e6)


# deserialize json
def read_json(filename):
    with open(filename, 'r') as f:
        return json.load(f)


# print json
def pretty_json(obj):
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ':'))


# get operator name type mapping
def name_type_mapping(query_plan_file):
    plan = read_json(query_plan_file)
    fragments = plan['fragments']
    mapping = dict()
    for fragment in fragments:
        for operator in fragment['operators']:
            if operator['op_name'] in mapping:
                print >> sys.stderr, "       dup names "
                sys.exit(1)
            else:
                mapping[operator['op_name']] = operator['op_type']
    return mapping


# get number of fragment of a query plan
def num_of_fragment(query_plan_file):
    plan = read_json(query_plan_file)
    fragments = plan['fragments']
    return len(fragments)


# build parent mapping
def get_parent(fragment):
    ret = dict()
    for operator in fragment['operators']:
        for field in children[operator['op_type']]:
            if not isinstance(operator[field], list):
                ret[operator[field]] = operator['op_name']
            else:
                for child in operator[field]:
                    ret[child] = operator['op_name']
    return ret


# build child list dictionary
def get_children(fragment):
    ret = defaultdict(list)
    for operator in fragment['operators']:
        for field in children[operator['op_type']]:
            if not isinstance(operator[field], list):
                ret[operator['op_name']].append(operator[field])
            else:
                for op in operator[field]:
                    ret[operator['op_name']].append(op)
    return ret


# build operator state
def build_operator_state(op_name, operators, children_dict,
                         type_dict, start_time):

    # build state from events
    states = []
    last_state = 'null'
    last_time = 0

    '''state:  0-compute, 1-sleep, 2-wait, 3-send, 4-receive'''
    for event in operators[op_name]:
        if last_state != 'null':
            if last_state == 'live' or last_state == 'wake':
                state = {
                    'begin': last_time-start_time,
                    'end': event['time']-start_time,
                    'name': 0
                }
                if event['message'] == 'hang':
                    state['tp_num'] = event['tp_num']
            elif last_state == 'wait':
                state = {
                    'begin': last_time-start_time,
                    'end': event['time']-start_time,
                    'name': 2
                }
            elif last_state == 'hang':
                state = {
                    'begin': last_time-start_time,
                    'end': event['time']-start_time,
                    'name': 1
                }
            states.append(state)
        last_state = event['message']
        last_time = event['time']

    children_ops = []

    for op in children_dict[op_name]:
        children_ops.append(
            build_operator_state(op, operators, children_dict,
                                 type_dict, start_time))

    qf = {
        'type': type_dict[op_name],
        'name': op_name,
        'states': states,
        'children': children_ops
    }

    return qf

def getRecoveryTaskStructure(operators, type_dict):
    for k, v in operators.items():
        if k.startswith("tupleSource_for_"):
            opName1 = k
            type_dict[k] = "TupleSource"
        if k.startswith("recProducer_for_"):
            opName2 = k
            type_dict[k] = "RecoverProducer"
    parent = dict()
    parent[opName1] = opName2
    children_dict  = defaultdict(list)
    children_dict[opName2].append(opName1)
    return (parent, children_dict, type_dict)

# unify time
def unify_time(viz_json):
    start_time = min(qf['real_begin'] for qf in viz_json['hierarchy'])
    viz_json['real_begin'] = start_time
    for root in viz_json['hierarchy']:
        unify_operator_time(root['real_begin']-start_time, root)


# unify operator time
def unify_operator_time(offset, operator):
    # update its own states
    for state in operator['states']:
        state['begin'] = state['begin']+offset
        state['end'] = state['end']+offset

    # update its children's states
    for child_op in operator['children']:
        unify_operator_time(offset, child_op)

    if 'begin' in operator:
        operator['begin'] = operator['begin']+offset
        operator['end'] = operator['end']+offset


# handle recovery task
def getRecoveryTaskStructure(operators, type_dict):
    for k, v in operators.items():
        if k.startswith("tupleSource_for_"):
            opName1 = k
            type_dict[k] = "TupleSource"
        if k.startswith("recProducer_for_"):
            opName2 = k
            type_dict[k] = "RecoverProducer"
    parent = dict()
    parent[opName1] = opName2
    children_dict = defaultdict(list)
    children_dict[opName2].append(opName1)
    return (parent, children_dict, type_dict)


# extract viz data of ?qf=fragment_id & worker=worker_id
def getOpLevelViz(path, worker_id, query_id,
                  fragment_id, query_plan_file, config_file):
    # get config info
    config = myriadeploy.read_config_file(config_file)
    workers = config['workers']

    # validate worker_id
    if worker_id > len(workers):
        raise Exception("worker_id {} is beyond range".format(worker_id))

    # validate fragment_id
    if fragment_id >= num_of_fragment(query_plan_file):
        raise Exception("Invalid fragment_id {}".format(fragment_id))

    fileName = "{}/query_{}_vis".format(path.rstrip('/'), query_id)
    query_data = pickle.load(open(fileName, "rb"))

    data = [qf for qf in query_data['hierarchy']
            if qf['worker_id'] == worker_id
            and qf['fragment_index'] == fragment_id]
    begin = min(int(qf['begin']) for qf in data)
    end = max(int(qf['end']) for qf in data)

    ans = {
        'begin': begin,
        'end': end,
        'hierarchy': data
    }

    print pretty_json(ans)


# get viz data of ?qf=fragment_id
def getQfLevelViz(path, query_id, fragment_id, query_plan_file, config_file):
    # validate fragment_id
    if fragment_id >= num_of_fragment(query_plan_file):
        raise Exception("Invalid fragment_id {}".format(fragment_id))

    # read query viz data
    fileName = "{}/query_{}_vis".format(path.rstrip('/'), query_id)
    query_data = pickle.load(open(fileName, "rb"))

    # filter relevant part
    data = [qf for qf in query_data['hierarchy']
            if qf['fragment_index'] == fragment_id]
    data = sorted(data, key=lambda qf: qf['worker_id'])

    begin = min(x['begin'] for x in data)
    end = max(x['end'] for x in data)

    # only keep root operators
    for qf in data:
        qf['children'] = []
        qf['name'] = "{} @ worker {}".format(qf['name'], qf['worker_id'])

    profile = {
        'begin': begin,
        'end': end,
        'hierarchy': data
    }

    print pretty_json(profile)


# generate viz data per query per worker
def generateSingleWorkerViz(path, worker_id, query_id,
                            query_plan_file, config_file):

    # get query information
    query = read_json(query_plan_file)

    # Workers are numbered from 1, not 0
    lines = [line.strip() for line in
             open("%s/worker_%i_profile" % (path.rstrip('/'), worker_id))]

    # parse information from each log message
    tuples = [re.findall(
<<<<<<< HEAD
        r'.query_id#(\d*)..([\w(),]*)@(-?\w*)..(\d*).:([\w|\W]*)', line)
=======
        r'.query_id#(\d*)..([\w(),]*)@(-?\w*)..(\d*)..(\d*).:([\w|\W]*)', line)
>>>>>>> 0681fb459c7bbdad5d770ceff3292d4cf5a53841
        for line in lines]

    tuples = [i[0] for i in tuples if len(i) > 0]
    tuples = [(i[1], {
        'time': long(i[3]),
        'query_id':i[0],
        'name':i[1],
        'fragment_id':i[2],
        'message':i[5],
        'tp_num': int(i[4])
    }) for i in tuples]

    ret = []

    # filter out unrelevant queries
    tuples = [i for i in tuples if int(i[1]['query_id']) == query_id]
    if len(tuples) == 0:
        raise Exception("cannot find information of query {} on worker {}"
                        .format(query_id, worker_id))
    for i, fragment in enumerate(query['fragments']):
        if ('workers' not in fragment or
                worker_id in map(int, fragment['workers'])):
            ret.append(generateFragmentViz(
                tuples, worker_id, i, query_plan_file, config_file))

    return ret


# generate viz data per query per worker per fragment
def generateFragmentViz(tuples, worker_id, fragment_id,
                        query_plan_file, config_file):

    # get name type mapping
    type_dict = name_type_mapping(query_plan_file)

    # get query_fragment start time
    mst = [i for i in tuples if i[0] == 'startTimeInMS'
           and int(i[1]['fragment_id']) == fragment_id]
    nst = [i for i in tuples if i[0] == 'startTimeInNS'
           and int(i[1]['fragment_id']) == fragment_id]

    qf_start_in_ms = mst[0][1]['time']
    qf_start_in_ns = nst[0][1]['time']

    # filter out unrelevant queries
    relevant_tuples = [
        i for i in tuples if int(i[1]['fragment_id']) == fragment_id
        and i[1]['message'] != 'set time']

    if len(relevant_tuples) == 0:
        raise Exception("Cannot get profiling information \
                        of worker {}".format(worker_id))

    # group by operator name
    operators = defaultdict(list)
    for tp in relevant_tuples:
        operators[tp[0]].append(tp[1])

<<<<<<< HEAD
    # get fragment tree structure
    if fragment_id < 0:
        # recovery tasks
        (parent, children_dict, type_dict) = getRecoveryTaskStructure(operators, type_dict)
    else:
        # normal fragments in the json query plan
        query_plan = read_json(query_plan_file)
        fragment = query_plan['fragments'][fragment_id]
        parent = get_parent(fragment)
        children_dict = get_children(fragment)
=======
    # get fragment information
    query_plan = read_json(query_plan_file)
    fragment = query_plan['fragments'][fragment_id]
    parent = get_parent(fragment)
    children_dict = get_children(fragment)
>>>>>>> 0681fb459c7bbdad5d770ceff3292d4cf5a53841

    # update parent's events
    induced_operators = defaultdict(list)
    for k, v in operators.items():
        if k in parent:
            for state in v:
                if state['message'] == 'live':
                    new_state = copy.deepcopy(state)
                    new_state['message'] = 'wait'
                    new_state['name'] = parent[k]
                    induced_operators[parent[k]].append(new_state)
                elif state['message'] == 'hang':
                    new_state = copy.deepcopy(state)
                    new_state['message'] = 'wake'
                    new_state['name'] = parent[k]
                    induced_operators[parent[k]].append(new_state)

    for k, v in induced_operators.items():
        operators[k].extend(v)
        operators[k] = sorted(operators[k], key=lambda k: k['time'])
        if type_dict[k] in root_operators:
            begin_time = operators[k][0]['time']
            end_time = operators[k][-1]['time']
            break
<<<<<<< HEAD
=======
        if(operators[k][0]['time']) < qf_start_in_ns:
            print k
            print operators[k][0]['time']
            print qf_start_in_ns
            raise Exception("wrong!")
>>>>>>> 0681fb459c7bbdad5d770ceff3292d4cf5a53841

    # build json
    for k, v in operators.items():
        if type_dict[k] in root_operators:
            data = build_operator_state(k, operators, children_dict, type_dict,
                                        begin_time)
            break

    data['real_begin'] = qf_start_in_ms*million+begin_time-qf_start_in_ns
    data['fragment_index'] = fragment_id
    data['worker_id'] = worker_id
    data['begin'] = 0
    data['end'] = end_time-begin_time
    return data


# generate visualization data per query
def generateViz(path, query_id, query_plan_file, config_file):
    # get configuration
    config = myriadeploy.read_config_file(config_file)
    workers = config['workers']

    # generate viz data from each worker and each query fragment
    viz_data = []
    for (i, worker) in enumerate(workers):
        viz_data.extend(
            generateSingleWorkerViz(path, i+1, query_id,
                                    query_plan_file, config_file))

    query_start_time = min(int(qf['real_begin']) for qf in viz_data)

    for qf in viz_data:
        unify_operator_time(qf['real_begin']-query_start_time, qf)

    end = max(qf['end'] for qf in viz_data)

    viz = {
        'begin': 0,
        'end': end,
        'hierarchy': viz_data
    }
    return viz


# write visualization data to disk if there no data written before
def writeViz(path, query_id, query_plan_file, config_file):
    fileName = "{}/query_{}_vis".format(path.rstrip('/'), query_id)
    if not os.path.isfile(fileName):
        viz = generateViz(path, query_id, query_plan_file, config_file)
        pickle.dump(viz, open(fileName, "wb"))


def main(argv):
# Usage
    if len(argv) != 6 and len(argv) != 7:
        print >> sys.stderr, \
            "Usage: %s <log_files_directory> <worker_id> <query_id> \
            <fragment_id> <query_plan_file> <config_file>" % (argv[0])
        print >> sys.stderr, \
            " or %s <log_files_directory>  <query_id> <fragment_id>\
            <query_plan_file> <config_file>" % (argv[0])
        print >> sys.stderr, "       log_file_directory "
        print >> sys.stderr, "       worker_id "
        print >> sys.stderr, "       query_id "
        print >> sys.stderr, "       fragment_id "
        print >> sys.stderr, "       query_plan_file "
        print >> sys.stderr, "       config file "
        sys.exit(1)
    elif len(argv) == 7:
        writeViz(argv[1], int(argv[3]), argv[5], argv[6])
        getOpLevelViz(argv[1], int(argv[2]), int(argv[3]),
                      int(argv[4]), argv[5], argv[6])
    else:
        writeViz(argv[1], int(argv[2]), argv[4], argv[5])
        getQfLevelViz(argv[1], int(argv[2]), int(argv[3]), argv[4], argv[5])


if __name__ == "__main__":
    main(sys.argv)
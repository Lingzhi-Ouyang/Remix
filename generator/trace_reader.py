#!/usr/bin/python3

# usage: python3 trace_reader.py -h

class TraceReader:

    def __init__(self):
        self._matching = {'{': ('}', self._braces), '<': ('$', self._chevrons),
                          '[': (']', self._brackets), '(': (')', self._parentheses)}

        self._string_dict = {"TRUE": True, "FALSE": False}
        self._user_dict = dict()
        self._kv_outside_handler = lambda k, v: (k, v)
        self._kv_inside_handler = lambda k, v: (k, v)
        self._list_handler = lambda s: s

    # find '}$])' for '{<[('
    def _find_next_match(self, string):
        level = 0
        char = string[0]
        match_char = self._matching[char][0]
        for i, c in enumerate(string):
            if c == char:
                level += 1
            elif c == match_char:
                level -= 1
                if level == 0:
                    return i
        assert False

    # < string $
    def _chevrons(self, string):
        l = list()
        if not string:
            return self._list_handler(l)
        processed, pos, length = 0, 0, len(string)
        while pos < length:
            if string[pos] in self._matching or string[pos] == ',':
                if string[pos] != ',':
                    pos = pos + self._find_next_match(string[pos:]) + 1
                l.append(self._variable_converter(string[processed:pos]))
                processed = pos + 2
                pos += 1
            pos += 1
        if pos != processed:
            l.append(self._variable_converter(string[processed:pos]))
        return self._list_handler(l)

    # { string } (we treat set as list)
    def _braces(self, string):
        return self._chevrons(string)

    # return a dict
    def _dict_common(self, string, arrow, sep, value_seq_len):
        d = dict()
        processed, pos, length = 0, 0, len(string)
        key, value = '', ''
        while True:
            if string[pos] == arrow[0]:
                key = string[processed:pos - 1]
                pos += len(arrow)
                processed = pos + 1
            if string[pos] == sep[0]:
                value = string[processed:pos - value_seq_len]
                pos += len(sep)
                processed = pos + 1
                key, value = self._kv_inside_handler(
                    key, self._variable_converter(value))
                d[key] = value
            pos += 1
            if pos >= length:
                break
            if string[pos] in self._matching:
                pos = pos + self._find_next_match(string[pos:])
        value = string[processed:]
        key, value = self._kv_inside_handler(
            key, self._variable_converter(value))
        d[key] = value
        return d

    # [ string ]
    def _brackets(self, string):
        return self._dict_common(string, '|->', ',', 0)

    # ( string )
    def _parentheses(self, string):
        return self._dict_common(string, ':>', '@@', 1)

    # convert string to python variable
    def _variable_converter(self, string):
        if string[0] in self._matching:
            return self._matching[string[0]][1](string[1:-1].strip())
        if string[0] == '"':
            return string[1:-1]
        if string in self._string_dict:
            return self._string_dict[string]
        if string in self._user_dict:
            return self._user_dict[string]
        try:
            return int(string)
        except ValueError:
            return string

    # callback handlers
    def set_user_dict(self, user_dict):
        self._user_dict = user_dict

    def set_kv_handler(self, kv_handler, inside=False):
        if inside:
            self._kv_inside_handler = kv_handler
        else:
            self._kv_outside_handler = kv_handler

    def set_list_handler(self, list_handler):
        self._list_handler = list_handler

    # convert MC.out to trace file
    @staticmethod
    def get_converted_string(file):
        if not hasattr(file, 'read'):
            f = open(file)
        else:
            f = file

        n_state = 0
        start_msg = 'The behavior up to this point is:'
        end_msg1 = 'Progress'
        end_msg2 = 'The number of states generated:'
        for line in f:
            if line[0] != '@':
                start_msg = 'Error: ' + start_msg
            break
        for line in f:
            if line.startswith(start_msg):
                yield '-' * 16 + ' MODULE MC_trace ' + '-' * 16 + '\n'
                break
        for line in f:
            if line[0] in '/ ':
                yield line
            elif line.startswith('State') or line[0].isdigit():
                n_state = n_state + 1
                yield 'STATE_{} == \n'.format(n_state)
            elif line == '\n':
                yield '\n' * 2
            elif line.startswith(end_msg1) or line.startswith(end_msg2):
                yield '=' * 49 + '\n'
                break
            else:
                if line[0] != '@':
                    assert False

        f.close()

    # read trace file and yield states as python objects
    def trace_reader_with_state_str(self, file):
        if not hasattr(file, 'read'):
            f = open(file)
        else:
            f = file

        if f.read(2) != '--':
            f = self.get_converted_string(f)

        state = dict()
        variable = ""
        lines = []
        for line in f:
            if line[0] in "-=S":
                if state:
                    # states.append(state)

                    yield state, ''.join(lines).strip()
                    state = dict()
                    lines = []
                continue
            elif line[0] in "/\n":
                if variable:
                    k, v = variable.split('=')
                    k, v = k.rstrip(), v.lstrip()
                    # replace to 1-char keywords, replace '>' to a uniq key
                    k, v = self._kv_outside_handler(k, self._variable_converter(
                        v.replace('<<', '<').replace('>>', '$')))
                    state[k] = v
                variable = line.strip()[3:]
                lines.append(line)
            else:
                variable += " " + line.strip()
                lines.append(line)

        f.close()

    def trace_reader(self, file):
        for state, _ in self.trace_reader_with_state_str(file):
            yield state


def post_processing(states):
    ### -> all possible actions
    # Election: 
        # FLEReceiveNotmsg(i, j), FLENotmsgTimeout(i), FLEHandleNotmsg(i), FLEWaitNewNotmsg(i)
    # Discovery:
        # ConnectAndFollowerSendFOLLOWERINFO(i, j), LeaderProcessFOLLOWERINFO(i, j), FollowerProcessLEADERINFO(i, j), LeaderProcessACKEPOCH(i, j)
    # Sync: 
        # LeaderSyncFollower(i, j), FollowerProcessSyncMessage(i, j), FollowerProcessPROPOSALInSync(i, j), FollowerProcessCOMMITInSync(i, j), FollowerProcessNEWLEADER(i, j), LeaderProcessACKLD(i, j), FollowerProcessUPTODATE(i, j), FollowerProcessNEWLEADERAfterCurrentEpochUpdated(i, j)
    # Broadcast: 
        # LeaderProcessRequest(i), FollowerProcessPROPOSAL(i, j), LeaderProcessACK(i, j), FollowerProcessCOMMIT(i, j)
        # FollowerSyncProcessorLogRequest(i, j), FollowerCommitProcessorCommit(i, j)
    # Failures: 
        # PartitionStart(i, j), PartitionRecover(i, j), NodeCrash(i), NodeStart(i), 
        # FilterNonexistentMessage(i)

    actions_without_peer = {'FLENotmsgTimeout', 'FLEHandleNotmsg', 'FLEWaitNewNotmsg',
                            'LeaderProcessRequest', 
                            'NodeCrash', 'NodeStart', 'FilterNonexistentMessage'}
    
    variables_no_record = {'rcvBuffer', 'leaderOracle', 'epochLeader', 'proposalMsgsLog', 'daInv',
                           'aaInv', 'committedLog', 'initialHistory', 'tempMaxEpoch', 'recorder'}

    # for simplified format output
    action_list = []

    state_list = []
    last_state = dict()

    inv_violate_str = ''
    inv_violate = False

    for num, state in enumerate(states):
        if num == 0:
            server_state = {'version': args.version, 'server_num': len(state['state'].keys()), 'server_id': list(state['state'].keys())}
            # action_list.append(server_state)
            state_list.append(server_state)

        cur_pc = state['recorder']['pc']
        if cur_pc[0] == 'Init':
            continue

        action_list.append(state['recorder']['pc'])

        k = cur_pc[0]
        v = dict()
        v['nodeId'] = cur_pc[1]

        # distinguish the actions w/o. peerId & extra parameters.  
        if k not in actions_without_peer:
            # print(k)
            v['peerId'] = cur_pc[2]
            if len(cur_pc) > 3: 
                v['coreParam'] = cur_pc[3:]
        elif len(cur_pc) > 2: 
            v['coreParam'] = cur_pc[2:]

        v['variables'] = dict()
        for var, value in state.items():
            if var not in variables_no_record:
                v['variables'][var] = value

        new_state = dict()
        new_state['Step'] = num
        new_state[k] = v
        state_list.append(new_state)

        # Check for invariant violations including DuringActionInvariant and AfterActionInvariant.
        # Rename the output trace with corresponding violated invariants.  
        if 'daInv' in state and 'aaInv' in state:
            for inv, val in state['daInv'].items():
                if val is False:
                    inv_violate_str = inv_violate_str + '-' + inv
                    print('Violation found: %s ' % inv_violate_str)
            for inv, val in state['aaInv'].items():
                if val is False:
                    inv_violate_str = inv_violate_str + '-' + inv
                    print('Violation found: %s ' % inv_violate_str)

            if False in state['daInv'].values() or False in state['aaInv'].values():
                inv_violate = True
                break

    return action_list, state_list, inv_violate, inv_violate_str


if __name__ == '__main__':
    import json
    import argparse

    # arg parser
    parser = argparse.ArgumentParser(
        description="Read TLA traces into Python objects")

    parser.add_argument(dest='trace_file', action='store',
                        help='TLA trace file')
    parser.add_argument('-o', dest='output_file_prefix', action='store', required=False,
                        help="output to json file with state sequence if invariants false")
    parser.add_argument('-f', dest='force', action='store', required=False,
                        help="force output to json file if true")
    parser.add_argument('-i', dest='indent', action='store', required=False,
                        type=int, help="json file indent")
    parser.add_argument('-v', dest='version', action='store', required=True,
                        type=str, help="spec version")
    args = parser.parse_args()

    tr = TraceReader()

    # set_user_dict and set_kv_handler usage example
    # tr.set_user_dict({"Nil": None})

    # def kv_handler(k, v):
    #     if k != 'messages':
    #         return k, v
    #     v.sort(key=lambda i: i['seq'])
    #     return k, v

    # tr.set_kv_handler(kv_handler, False)

    states = list(tr.trace_reader(args.trace_file))
    actions, states, violate, inv_violate_str = post_processing(states)

    if args.output_file_prefix:
        if violate:
            print("--> Generate one test case that violates invariants:", args.output_file_prefix)
            with open(args.output_file_prefix + inv_violate_str + '.json', 'w') as f:
                json.dump(states, f, indent=args.indent)
                f.write('\n')
            with open(args.output_file_prefix + inv_violate_str + '.txt', 'w') as f:
                for counter, action in enumerate(actions):
                    f.write(str(counter+1) + '\t' + str(action))
                    f.write('\n')
        if args.force and not violate:
            print("--> Generate one test case that does not violate invariants:", args.output_file_prefix)
            with open(args.output_file_prefix + '.json', 'w') as f:
                json.dump(states, f, indent=args.indent)
                f.write('\n')
            with open(args.output_file_prefix + '.txt', 'w') as f:
                for counter, action in enumerate(actions):
                    f.write(str(counter+1) + '\t' + str(action))
                    f.write('\n')
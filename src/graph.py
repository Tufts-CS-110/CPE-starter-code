##### Starter template for graph.py
# This version mirrors the structure of the original repository,
# but the critical-path algorithm (`computeCriticalPath`) has been
# left unimplemented for you to fill in as described by the handout.

import logging
from collections import deque

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
debug_on = logging.getLogger(__name__).isEnabledFor(logging.DEBUG)

# constants used throughout the code
_SPANS = 'spans'
_SPAN_ID = 'spanID'
_REFERENCES = 'references'
_START_TIME = 'startTime'
_DURATION = 'duration'
_OPERATION_NAME = 'operationName'
_PROCESS_ID = 'processID'
_TRACE_ID = 'traceID'
_REF_TYPE = 'refType'
_CHILD_OF = 'CHILD_OF'
_TAGS = 'tags'
_PROCESSES = 'processes'
_HOSTNAME = 'hostname'
_TESTING = 'testing'
_OVERLAP_ALLOWANCE_FRACTION = 0.01


class GraphNode():
    """Lightweight wrapper for a Jaeger span."""
    def __init__(self, sid, startTime, duration, parentSpanId, opName,
                 processID, serviceName):
        self.sid = sid
        self.startTime = startTime
        self.originalStartTime = startTime
        self.duration = duration
        self.originalDuration = duration
        self.parentSpanId = parentSpanId
        self.endTime = startTime + duration
        self.parent = None
        self.opName = opName
        self.pid = processID
        self.children = {}
        self.on_critical_path = False
        self.serviceName = serviceName

    def setParent(self, parent):
        self.parent = parent

    def addChild(self, child):
        self.children[child] = True

    def __repr__(self):
        return f'Span(ServiceName={self.serviceName}, OperationName={self.opName})'


class CPEvent():
    """A timestamped event used for debugging/visualization.
    Students do not need to manipulate these objects directly in
    their implementation but the helper methods may refer to them.
    """
    def __init__(self, timestamp, name, prev):
        self.timestamp = timestamp
        self.name = name
        self.prev = prev

    def __repr__(self):
        return f"{self.name}"


class Graph():
    """In-memory representation of a trace and associated helpers.

    Only a subset of the original class is provided here; additional
    methods from the full version may be copied over as needed while
    implementing `computeCriticalPath`.
    """
    def __init__(self, data, serviceName, operationName, filename,
                 rootTrace) -> None:
        self.operationName = operationName
        self.serviceName = serviceName
        self.filename = filename
        self.rootNode = None
        self.nodeHT = {}
        self.processName = {}
        self.hostMap = {}
        self.totalShrink = 0
        self.totalDrop = 0
        self.shrinkCounter = 0
        self.testing = {}
        self.exclusiveExampleMap = {}
        self.inclusiveExampleMap = {}
        self.callChain = {}
        self.last_CP_event = None
        self.cur_CP_event = None
        self.end_Node = None
        self.seq_Node_stack = deque()

        # parsing logic is omitted here; refer to the full source if you
        # need to construct a graph from JSON data.

    def computeCriticalPath(self, curNode):
        """Return a list of nodes on the critical path rooted at `curNode`.

        You should implement the recursive algorithm described in the
        project handout.  The helper methods defined elsewhere in this
        template can be used if desired.
        """
        # -------------------------------------------------------------
        # TODO: your implementation goes here.  The autograder will call
        #       this method directly when evaluating your submission.
        # ------------------------------------------------------------_
        pathReturn = []

        # Guard if curNode None
        if curNode is None:
            return []

        if self.last_CP_event is None:
            end_event = CPEvent(
                curNode.endTime,
                curNode.serviceName + "_" + curNode.opName + "_end",
                None,
            )
            self.last_CP_event = end_event
            self.cur_CP_event = end_event
        else:
            self.create_cp_event(curNode, "_end")

        curNode.on_critical_path = True
        pathReturn.append(curNode)

        children = list(curNode.children.keys())

        if len(children) == 0:
            self.end_Node = curNode
            return pathReturn

        # Sort children by end time
        sortedChildren = sorted(children, key=lambda c: c.endTime, reverse=True)
        active_child = sortedChildren[0]
        self.create_cp_seg_event(active_child, "_seg_start")
        pathReturn.extend(self.computeCriticalPath(active_child))

        # Rest of children
        for i in range(1, len(sortedChildren)):
            c = sortedChildren[i]
            if self.happensBefore(curNode, sortedChildren, c, active_child):
                cur = self.end_Node
                while cur != curNode:
                    self.create_cp_event(cur, "_start")
                    self.create_cp_seg_event(cur, "_seg_end")
                    cur = cur.parent

                self.create_cp_seg_event(c, "_seg_start")
                active_child = c
                pathReturn.extend(self.computeCriticalPath(c))

        return pathReturn


    def printCPEvents(self):
        cur = self.last_CP_event
        result = f"({cur.name}, {cur.timestamp})"
        while cur.prev:
            prev_label = f"({cur.prev.name}, {cur.prev.timestamp})"
            result = prev_label +  "\n-->\n"  + result
            cur = cur.prev
        return result

    def numSyncEventsInWindowInclusive(self, children, startTime, endTime):
        numEvents = 0
        for c in children:
            if c.startTime >= startTime and c.startTime <= endTime:
                numEvents = numEvents + 1
            if c.endTime >= startTime and c.endTime <= endTime:
                numEvents = numEvents + 1
        return numEvents

    def happensBefore(self, parent, reverseSortedChildren, childBefore,
                      childLater):
        # happensBefore returns true if the end of childBefore happens before the start of childLater.
        # however, there is some heuristic to accommodate clock skew.

        # obviously A HB B if
        # Astart------Aend Bstart-----Bend
        if childBefore.endTime < childLater.startTime:
            return True

        # allow a 1 % overlap with earlier child starting prior to the later child
        # Allow this:
        # Astart------Aend
        #            Bstart-----Bend
        # Don't allow this
        # Astart-------Aend
        #            Bstart-----Bend
        #     Cstart---Cend
        if (childBefore.endTime < childLater.endTime) and (
                childBefore.startTime < childLater.startTime) and (
                (childBefore.endTime - childLater.startTime) /
                parent.duration < _OVERLAP_ALLOWANCE_FRACTION):
            # Now check that there is no other overlapping child in this region
            nEvt = self.numSyncEventsInWindowInclusive(reverseSortedChildren,
                                                       childLater.startTime,
                                                       childBefore.endTime)
            debug_on and logging.debug(
                f"nEvt for {self.canonicalOpName(childBefore)} = {nEvt}")
            if nEvt == 2:  # there can two and only 2 events in this window
                return True
        return False

    def check_seq_sibling(self, curNode, sortedChildren, child_index, lrc):
        for cn in sortedChildren[child_index:]:
            if self.happensBefore(curNode, sortedChildren, cn, lrc):
                return True
        return False

    def create_cp_event(self, curNode, s_or_e):
        if s_or_e == "_start":
            timestamp = curNode.startTime
        else:
            timestamp = curNode.endTime
        name = curNode.serviceName + "_" + curNode.opName + s_or_e
        cur_new = CPEvent(timestamp, name, None)
        self.cur_CP_event.prev = cur_new
        self.cur_CP_event = cur_new

    def create_cp_seg_event(self, curNode, s_or_e):
        if s_or_e == "_seg_start":
            timestamp = curNode.endTime
        else:
            timestamp = curNode.startTime

        p_node = curNode.parent
        name = p_node.serviceName + "_" + p_node.opName + s_or_e
        parent_seg = CPEvent(timestamp, name, None)
        self.cur_CP_event.prev = parent_seg
        self.cur_CP_event = parent_seg

    def add_return_to_seq_nodes(self, curNode):
        seq_node = self.seq_Node_stack.pop()
        while curNode != seq_node.parent:
            self.create_cp_event(curNode, "_start")
            self.create_cp_seg_event(curNode, "_seg_end")
            curNode = curNode.parent

    def complete_CP_events(self):
        curNode = self.end_Node

        while curNode != self.rootNode:
            self.create_cp_event(curNode, "_start")
            self.create_cp_seg_event(curNode, "_seg_end")
            curNode = curNode.parent

        # handle root
        if len(self.rootNode.children) != 0:
            self.create_cp_event(curNode, "_start")

    def output_cpe_txt(self, filename):
        events = self.printCPEvents()
        with open(filename, 'w') as file:
            file.write(events)
        print(f"cpe *txt* version saved to {filename}")

    def output_cpe_dot(self, filename):
        """
        Generate a DOT language file for a sequence of CPEvent objects with time differences as edge labels.
        """
        with open(filename, 'w') as f:
            f.write("digraph CriticalPath {\n")
            f.write("    rankdir=TB;\n")
            event_ids = {}
            id_counter = 0
            def get_event_id(event):
                nonlocal id_counter
                if event not in event_ids:
                    event_ids[event] = f"event_{id_counter}"
                    id_counter += 1
                return event_ids[event]
            cur = self.last_CP_event
            while cur.prev:
                cur_id = get_event_id(cur)
                prev_id = get_event_id(cur.prev)
                time_diff = int(cur.timestamp) - int(cur.prev.timestamp)
                f.write(f'    "{cur_id}" [label="{cur.name}\n{cur.timestamp}"];\n')
                f.write(f'    "{prev_id}" -> "{cur_id}" [label="{time_diff}"];\n')
                f.write(f'    "{prev_id}" [label="{cur.prev.name}\n{cur.prev.timestamp}"];\n')
                cur = cur.prev
            if cur:
                cur_id = get_event_id(cur)
                f.write(f'    "{cur_id}" [label="{cur.name}\n{cur.timestamp}"];\n')
            f.write("}\n")
        print(f"cpe *dot* version saved to {filename}")

    def findCriticalPath(self):
        cp = self.computeCriticalPath(self.rootNode)
        self.complete_CP_events()
        return cp

    def canonicalOpName(self, node):
        return '[' + self.processName[node.pid] + '] ' + node.opName


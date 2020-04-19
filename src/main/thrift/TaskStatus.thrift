## TaskStatus
# Defines a struct denoting the Status of a Task.

include "Timestamp.thrift"

namespace java page.parekh.scalingstream.taskrpc

typedef binary Timestamp

struct TaskState {
    1: Timestamp timestamp,
    2: bool isRunning,
    3: bool isDone,
    4: i32 numConsumed,
    5: i32 numProduced,
}


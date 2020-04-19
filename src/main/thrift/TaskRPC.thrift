## TaskRPC
# Defines the communication interface for a deployed Task.

include "TaskStatus.thrift"

namespace java page.parekh.scalingstream.taskrpc

service TaskRPC {
    binary taskID(),
    string operatorID(),
    bool isRunning(),
    bool isDone(),
    i32 numConsumed(),
    i32 numProduced(),
    TaskStatus.TaskState getStatus(),
    void ping(),
    void kill(),
    oneway void run()
}

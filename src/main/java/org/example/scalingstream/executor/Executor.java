package org.example.scalingstream.executor;

import org.example.scalingstream.dag.DAG;

import java.util.List;

public interface Executor {
    void exec(DAG dag);
}



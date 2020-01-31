package org.example.scalingstream.dag

interface DAG : List<List<Runnable>> {

    fun popFirst() : List<Runnable>;

}
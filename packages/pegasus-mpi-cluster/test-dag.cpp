#include <string>
#include <stdio.h>

#include "stdlib.h"
#include "dag.h"
#include "failure.h"
#include "log.h"

using std::exception;

void test_dag() {
    DAG dag("test/test.dag");
    
    Task *alpha = dag.get_task("Alpha");
    if (alpha == NULL) {
        myfailure("Didn't parse Alpha");
    }
    if (alpha->args.front().compare("/bin/echo") != 0) {
        myfailure("Command failed for Alpha: %s", alpha->args.front().c_str());
    }
    
    Task *beta = dag.get_task("Beta");
    if (beta == NULL) {
        myfailure("Didn't parse Beta");
    }
    if (beta->args.front().compare("/bin/echo") != 0) {
        myfailure("Command failed for Beta: %s", beta->args.front().c_str());
    }
    
    if (alpha->children[0] != beta) {
        myfailure("No children");
    }
    
    if (beta->parents[0] != alpha) {
        myfailure("No parents");
    }
}

void test_rescue() {
    DAG dag("test/diamond.dag", "test/diamond.rescue");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (!a->success) {
        myfailure("A should have been successful");
    }

    if (!b->success) {
        myfailure("B should have been successful");
    }

    if (!c->success) {
        myfailure("C should have been successful");
    }

    if (d->success) {
        myfailure("D should have been failed");
    }
}

void test_pegasus_dag() {
    DAG dag("test/pegasus.dag");
    
    Task *a = dag.get_task("A");
    
    if (a->pegasus_id.compare("1") != 0) {
        myfailure("A should have had pegasus_id");
    }
    /*
    if (a->pegasus_transformation.compare("mDiffFit:3.3") != 0) {
        myfailure("A should have had pegasus_transformation");
    }
    */
    
    Task *b = dag.get_task("B");
    
    if (b->pegasus_id.compare("2") != 0) {
        myfailure("B should have had pegasus_id");
    }
    /*
    if (b->pegasus_transformation.compare("mDiff:3.3") != 0) {
        myfailure("B should have had pegasus_transformation");
    }
    */
}

void test_memory_dag() {
    DAG dag("test/memory.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->memory != 0) {
        myfailure("A should require 0 MB memory");
    }
    
    if (b->memory != 100) {
        myfailure("B should require 100 MB memory");
    }
    
    if (c->memory != 100) {
        myfailure("C should require 100 MB memory");
    }
    
    if (d->memory != 100) {
        myfailure("D should require 100 MB memory");
    }
}

void test_cpu_dag() {
    DAG dag("test/cpus.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->cpus != 1) {
        myfailure("A should require 1 CPUs");
    }
    
    if (b->cpus != 2) {
        myfailure("B should require 2 CPUs");
    }
    
    if (c->cpus != 2) {
        myfailure("C should require 2 CPUs");
    }
    if (c->memory != 100) {
        myfailure("C should require 100 MB memory");
    }
    
    if (d->cpus != 2) {
        myfailure("D should require 2 CPUs");
    }
}

void test_tries_dag() {
    DAG dag("test/tries.dag", "", true, 3);
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    Task *d = dag.get_task("D");
    
    if (a->tries != 2) {
        myfailure("A should have 2 tries");
    }
    
    if (b->tries != 5) {
        myfailure("B should have 5 tries");
    }
    
    if (c->tries != 3) {
        myfailure("C should have 3 tries");
    }
    
    if (d->tries != 2) {
        myfailure("D should have 2 tries");
    }
    if (d->memory != 100) {
        myfailure("D should require 100 MB memory");
    }
}

void test_priority_dag() {
    DAG dag("test/priority.dag");
    
    Task *g = dag.get_task("G");
    Task *i = dag.get_task("I");
    Task *d = dag.get_task("D");
    Task *e = dag.get_task("E");
    Task *o = dag.get_task("O");
    Task *n = dag.get_task("N");
    
    if (g->priority != 10) {
        myfailure("G should have priority 10");
    }
    
    if (i->priority != 9) {
        myfailure("I should have priority 9");
    }
    
    if (d->priority != 8) {
        myfailure("D should have priority 8");
    }
    
    if (e->priority != 7) {
        myfailure("E should have priority 7");
    }
    
    if (o->priority != -4) {
        myfailure("O should have priority -4");
    }
    
    if (n->priority != -5) {
        myfailure("N should have priority -5");
    }
}

void test_pipe_forward() {
    DAG dag("test/forward.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");

    map<string,string> *fwds;
    
    fwds = a->pipe_forwards;
    if (fwds->size() != 1) {
        myfailure("A should have one forward");
    }
    if ((*fwds)["FOO"] != "./test/forward.dag.foo") {
        myfailure("A should be forwarding foo");
    }
    
    fwds = b->pipe_forwards;
    if (fwds->size() != 1) {
        myfailure("B should have one forward");
    }
    if ((*fwds)["BAR"] != "./test/forward.dag.bar") {
        myfailure("B should be forwarding bar");
    }

    fwds = c->pipe_forwards;
    if (fwds->size() != 2) {
        myfailure("C should have two forwards");
    }
    if ((*fwds)["FOO"] != "./test/forward.dag.foo" && 
        (*fwds)["BAR"] != "./test/forward.dag.bar") {
        myfailure("C should be forwarding foo and bar");
    }
}

void test_file_forward() {
    DAG dag("test/file_forward.dag");
    
    Task *a = dag.get_task("A");
    Task *b = dag.get_task("B");
    Task *c = dag.get_task("C");
    
    map<string,string> *fwds;
    
    fwds = a->file_forwards;
    if (fwds->size() != 1) {
        myfailure("A should have one forward");
    }
    if ((*fwds)["./test/scratch/foo"] != "./test/forward.dag.foo") {
        myfailure("A should be forwarding foo");
    }
    
    fwds = b->file_forwards;
    if (fwds->size() != 1) {
        myfailure("B should have one forward");
    }
    if ((*fwds)["./test/scratch/bar"] != "./test/forward.dag.bar") {
        myfailure("B should be forwarding bar");
    }
    
    fwds = c->file_forwards;
    if (fwds->size() != 2) {
        myfailure("C should have two forwards");
    }
    if ((*fwds)["./test/scratch/foo"] != "./test/forward.dag.foo" && 
        (*fwds)["./test/scratch/bar"] != "./test/forward.dag.bar") {
        myfailure("C should be forwarding foo and bar");
    }
}

int main(int argc, char *argv[]) {
    try {
        log_set_level(LOG_ERROR);
        test_dag();
        test_rescue();
        test_pegasus_dag();
        test_memory_dag();
        test_cpu_dag();
        test_tries_dag();
        test_priority_dag();
        test_pipe_forward();
        test_file_forward();
        return 0;
    } catch (exception &error) {
        log_error("ERROR: %s", error.what());
        return 1;
    }
}

#ifndef MASTER_H
#define MASTER_H

#include <list>
#include <vector>
#include <map>

#include "engine.h"
#include "dag.h"
#include "protocol.h"
#include "comm.h"
#include "fdcache.h"

using std::string;
using std::vector;
using std::priority_queue;
using std::list;
using std::map;

class Host {
private:
    Task **cpus;

    string host_name;
    unsigned int memory;
    cpu_t threads;
    cpu_t cores;
    cpu_t sockets;
    unsigned int slots;

    unsigned int memory_free;
    unsigned int cpus_free;
    unsigned int slots_free;

public:
    Host(const string &host_name, unsigned int memory, cpu_t threads, cpu_t cores, cpu_t sockets);
    ~Host();
    const char *name() { return host_name.c_str(); }
    void add_slot();
    bool can_run(Task *task);
    vector<cpu_t> allocate_resources(Task *task);
    void release_resources(Task *task);
    void log_resources(FILE *resource_log);
};

class Slot {
public:
    unsigned int rank;
    Host *host;
    
    Slot(unsigned int rank, Host *host) {
        this->rank = rank;
        this->host = host;
    }
};

class TaskPriority {
public:
    bool operator ()(const Task *x, const Task *y){
        return x->priority < y->priority;
    }
};

typedef enum {
    WORKFLOW_START,
    WORKFLOW_SUCCESS,
    WORKFLOW_FAILURE,
    TASK_QUEUED,
    TASK_SUBMIT,
    TASK_SUCCESS,
    TASK_FAILURE
} WorkflowEvent;

class WorkflowEventListener {
public:
    virtual void on_event(WorkflowEvent event, Task *task) = 0;
};

class JobstateLog : public WorkflowEventListener {
private:
    string path;
    FILE *logfile;
    
    void open();
    void close();
public:
    JobstateLog(const string &path);
    ~JobstateLog();
    void on_event(WorkflowEvent event, Task *task);
};

class DAGManLog : public WorkflowEventListener {
private:
    string logpath;
    string dagpath;
    FILE *logfile;
    
    void open();
    void close();
public:
    DAGManLog(const string &logpath, const string &dagpath);
    ~DAGManLog();
    void on_event(WorkflowEvent event, Task *task);
};

typedef priority_queue<Task *, vector<Task *>, TaskPriority> TaskQueue;

typedef list<Slot *> SlotList;
typedef list<Task *> TaskList;

class Master {
    Communicator *comm;
    
    string program;
    string dagfile;
    string outfile;
    string errfile;
    DAG *dag;
    Engine *engine;
    
    FILE *resource_log;
    
    vector<Slot *> slots;
    vector<Host *> hosts;
    SlotList free_slots;
    TaskQueue ready_queue;
    
    int numworkers;
    double max_wall_time;
    
    unsigned submitted_count;
    unsigned success_count;
    unsigned failed_count;
    
    unsigned total_cpus;
    double total_runtime;
    
    bool has_host_script;
    
    double start_time;
    double finish_time;
    double wall_time;
    
    FDCache *fdcache;
    
    bool per_task_stdio;
    
    list<WorkflowEventListener *> listeners;
    unsigned task_submit_seq;
    
    void register_workers();
    void schedule_tasks();
    void wait_for_results();
    void process_result(ResultMessage *mesg);
    void process_iodata(IODataMessage *mesg);
    void queue_ready_tasks();
    void submit_task(Task *t, int worker, const vector<cpu_t> &bindings);
    void merge_all_task_stdio();
    void merge_task_stdio(FILE *dest, const string &src, const string &stream);
    void write_cluster_summary(bool failed);

    void publish_event(WorkflowEvent event, Task *task);
    bool wall_time_exceeded();
public:
    Master(Communicator *comm, const string &program, Engine &engine, DAG &dag, const string &dagfile, 
        const string &outfile, const string &errfile, bool has_host_script = false, 
        double max_wall_time = 0.0, const string &resourcefile = "", bool per_task_stdio = false,
        int maxfds = 0);
    ~Master();
    int run();
    void add_listener(WorkflowEventListener *l);
};

#endif /* MASTER_H */

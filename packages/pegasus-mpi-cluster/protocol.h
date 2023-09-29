#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <string>
#include <map>
#include <list>
#include <vector>

#include "tools.h"

using std::string;
using std::map;
using std::list;
using std::vector;

enum MessageType {
    COMMAND      = 1,
    RESULT       = 2,
    SHUTDOWN     = 3,
    REGISTRATION = 4,
    HOSTRANK     = 5,
    IODATA       = 6
};

class Message {
public:
    int source;
    char *msg;
    unsigned msgsize;

    Message();
    Message(char *msg, unsigned msgsize, int source);
    virtual ~Message();
    virtual int tag() const = 0;
};

class ShutdownMessage: public Message {
public:
    ShutdownMessage(char *msg, unsigned msgsize, int source);
    ShutdownMessage();
    virtual int tag() const { return SHUTDOWN; };
};

class CommandMessage: public Message {
public:
    string name;
    list<string> args;
    string id;
    unsigned memory;
    cpu_t cpus;
    vector<cpu_t> bindings;
    map<string, string> pipe_forwards;
    map<string, string> file_forwards;

    CommandMessage(char *msg, unsigned msgsize, int source);
    CommandMessage(const string &name, const list<string> &args, const string &id, unsigned memory, cpu_t cpus, const vector<cpu_t> &bindings, const map<string,string> *pipe_forwards, const map<string,string> *file_forwards);
    virtual int tag() const { return COMMAND; };
};

class ResultMessage: public Message {
public:
    string name;
    int exitcode;
    double runtime;

    ResultMessage(char *msg, unsigned msgsize, int source, int _dummy_);
    ResultMessage(const string &name, int exitcode, double runtime);
    virtual int tag() const { return RESULT; };
};

class RegistrationMessage: public Message {
public:
    string hostname;
    unsigned memory;
    cpu_t threads;
    cpu_t cores;
    cpu_t sockets;

    RegistrationMessage(char *msg, unsigned msgsize, int source);
    RegistrationMessage(const string &hostname, unsigned memory, cpu_t threads, cpu_t cores, cpu_t sockets);
    virtual int tag() const { return REGISTRATION; };
};

class HostrankMessage: public Message {
public:
    int hostrank;

    HostrankMessage(char *msg, unsigned msgsize, int source);
    HostrankMessage(int hostrank);
    virtual int tag() const { return HOSTRANK; };
};

class IODataMessage: public Message {
public:
    string task;
    string filename;
    const char *data;
    unsigned size;

    IODataMessage(char *msg, unsigned msgsize, int source);
    IODataMessage(const string &task, const string &filename, const char *data, unsigned size);
    virtual int tag() const { return IODATA; }
};

#endif /* PROTOCOL_H */


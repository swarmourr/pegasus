/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2016 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#include <sys/types.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <unistd.h>

#include "tools.h"
#include "parser.h"
#include "report.h"
#include "mysystem.h"
#include "job.h"
#include "statinfo.h"

int debug = 0;
int progress = -1;
char* application = "pegasus-cluster";
static char success[257];

/* purpose: write help message and exit
 * paramtr: programname (IN): application of the program (us)
 *           rc (IN): exit code to exit with
 * returns: procedure does not return. */
static void helpMe(const char* programname, int rc) {
    printf("Usage:\t%s [options] [inputfile]\n\n", programname);
    printf("Optional arguments:\n"
           " -d\tIncrease debug mode.\n"
           " -s fn\tProtocol anything to given status file, default stdout.\n"
           " -R fn\tRecords progress into the given file, see also SEQEXEC_PROGRESS_REPORT.\n"
           " -S ec\tMulti-option: Mark non-zero exit-code ec as success.\n"
           " -n nr\tNumber of CPUs to use, defaults to 1, string 'auto' permitted.\n"
           " input\tFile with list of applications and args to execute, default stdin.\n\n"
           "Execution control and exit code:\n"
           "\tExecute everything but return success only if all were successful.\n"
           " -e\tExecute everything (old default mode) and always return success.\n"
           " -f\tFail hard on first error (non-zero exit code or signal death).\n"
           "\tOption -e and -f are mutually exclusive.\n" );
    exit(rc);
}

static int processors() {
    /* TODO Does this work on OSX? */
#ifdef _SC_NPROCESSORS_CONF
    long config = sysconf(_SC_NPROCESSORS_CONF);
#else
    long config = 1;
#endif

#ifdef _SC_NPROCESSORS_ONLN
    long online = sysconf(_SC_NPROCESSORS_ONLN);
#else
    long online = 1;
#endif

    if (config <= 0) config = 1;
    if (online <= 0) online = 1;
    return config < online ? config : online;
}

static void parseCommandline(int argc, char *argv[], int *fail_hard, int *old_mode, int *cpus) {

    /* exit code 0 is always good, just in case */
    memset(success, 0, sizeof(success));
    success[0] = 1;

    /* Get application name */
    char *ptr = strrchr(argv[0], '/');
    if (ptr == NULL) {
        ptr = argv[0];
    } else {
        ptr++;
    }
    application = ptr;

    /* default progress report location */
    char *progress_file = getenv("SEQEXEC_PROGRESS_REPORT");

    /* Set default parallelism */
    char *cpus_string = getenv("SEQEXEC_CPUS");

    int option, tmp;
    opterr = 0;
    while ((option = getopt(argc, argv, "R:S:defhn:s:")) != -1) {
        switch (option) {
        case 'R':
            progress_file = optarg;
            break;
        case 'S':
            tmp = atoi(optarg);
            if (tmp > 0 && tmp < sizeof(success)) {
                success[tmp] = 1;
            } else {
                showerr("%s: Ignoring unreasonable success code: %d\n",
                        application, tmp);
            }
            break;
        case 'd':
            debug++;
            break;
        case 'e':
            *old_mode = 1;
            *fail_hard = 0;
            break;
        case 'f':
            *fail_hard = 1;
            *old_mode = 0;
            break;
        case 'n':
            cpus_string = optarg;
            break;
        case 's':
            if (freopen(optarg, "w", stdout) == NULL) {
                showerr("%s: open status %s: %d: %s\n",
                        application, optarg, errno, strerror(errno));
                exit(2);
            }
            break;
        case 'h':
        case '?':
            helpMe(ptr, 0);
            break;
        default:
            helpMe(ptr, 1);
            break;
        }
    }

    /* More than one argument left? Then error */
    if ((argc - optind) > 1) {
        helpMe(ptr, 1);
    }

    /* Open the progress file, if specified */
    if (progress_file != NULL) {
        progress = open(progress_file, O_WRONLY | O_APPEND | O_CREAT, 0666);
        if (progress == -1) {
            showerr("%s: open progress %s: %d: %s\n",
                    application, progress_file, errno, strerror(errno));
            exit(1);
        }
    }

    /* Set parallelism */
    if (cpus_string == NULL) {
        *cpus = 1;
    } else {
        if (strcasecmp(cpus_string, "auto") == 0) {
            *cpus = processors();
        } else {
            *cpus = atoi(cpus_string);
        }
        if (*cpus < 0) {
            *cpus = 1;
        }
    }

    /* If there is one argument left, then point stdin to it */
    if ((argc - optind) == 1) {
        if ((freopen(argv[optind], "r", stdin)) == NULL) {
            showerr("%s: open input %s: %d: %s\n",
                    application, argv[optind], errno, strerror(errno));
            exit(3);
        }
    }
}

/* purpose: merge two strings and return the result
 * paramtr: s1 (IN): first string, may be NULL
 *          s2 (IN): second string, must not be null
 *          use_space (IN): flag, if true, separate by space
 * returns: merge of strings into newly allocated area.
 */
static char *merge( char* s1, char* s2, int use_space ) {
    if (s1 == NULL) {
        return strdup(s2);
    } else {
        size_t l1 = strlen(s1);
        size_t l2 = strlen(s2);
        size_t len = l1 + l2 + 2;
        char* temp = (char*) malloc(len);
        strncpy( temp, s1, len );
        if ( use_space ) strncat( temp, " ", len );
        strncat( temp, s2, len );
        return temp;
    }
}

pid_t wait_for_child( Jobs* jobs, int* status ) {
    struct rusage usage;
    Signals save;
    int saverr;
    double final;
    size_t slot;
    pid_t child = ((pid_t) -1);

    /* FIXME: Not sure, if I need these. I take it to mean: While I am
     * blocked in kernel wait()ing for children, do not interrupt ^C me,
     * and do not send me SIGCHLD since I am inside wait() anyways. Do
     * send the signals to the children, though (which hopefully exit.)
     */
    save_signals(&save);
    errno = 0; /* we rely later on wait4 results */
    while ( (child = wait4( ((pid_t) 0), status, 0, &usage )) < 0 ) {
        saverr = errno;
        perror("wait4");
        errno = saverr;

        if (errno != EINTR) {
            *status = -1;
            break;
        }
    }
    saverr = errno;
    final = now(NULL);

    /* FIXME: see above, end bracket. */
    restore_signals(&save);

    /* find child that has finished */
    for (slot=0; slot < jobs->cpus; ++slot) {
        if (jobs->jobs[slot].child == child) break;
    }

    if ( slot == jobs->cpus ) {
        /* reaped child not found, not good */
        showerr("%s: process %d (status %d) is not a known child, ignoring.\n",
                application, child, *status);
    } else {
        /* free slot and report */
        char date[32];
        Job* j = (jobs->jobs) + slot;

        /* 20110419 PM-364: new requirement */
        showout("[cluster-task id=%lu, start=\"%s\", duration=%.3f, status=%d, "
                "line=%lu, pid=%d, app=\"%s\"]\n",
                j->count,
                iso2date( j->start, date, sizeof(date) ),
                (final - j->start),
                *status,
                j->lineno,
                child,
                j->argv[ find_application(j->argv) ] );

        /* progress report at finish of job */
        if (progress != -1) {
            report(progress, final, (final - j->start), *status, j->argv, &usage, NULL , j->count);
        }

        /* free reported job */
        job_done(j);
    }

    errno = saverr;
    return child;
}

void run_independent_task(char* cmd, char* envp[], unsigned long* extra, const char* special) {
    if (cmd == NULL) {
        return;
    }

    size_t len;
    int appc, other;
    char** appv;

    if ((appc = interpretArguments(cmd, &appv)) > 0) {
        /* determine full path to application according to PATH */
        char* fqpn = find_executable(appv[0]);
        if (fqpn) {
            /* found a FQPN, exchange first item in argument vector */
            free(appv[0]);
            appv[0] = fqpn;
        }

        other = mysystem(appv, envp, special);
        if (other || debug) {
            showerr("%s: %s returned %d/%d\n", application, special,
                    (other >> 8), (other & 127) );
        }
        for (len=0; len<appc; len++) {
            free(appv[len]);
        }
        free(appv);
    } else {
        /* unparsable cleanup argument string */
        showerr("%s: unparsable %s string, ignoring\n",
                application, special);
    }
    (*extra)++;
}

int isafailure(int status) {
    /* FIXME: On systems with exit codes outside 0..256 this may core dump! */
    return (WIFEXITED(status) && success[WEXITSTATUS(status)] == 1 ) ? 0 : 1;
}

void massage_failure(int fail_hard, int current_ec, int *collect_ec) {
    if (fail_hard) {
        /* only propagate first failure in hard-fail mode */
        if (!(*collect_ec && isafailure(*collect_ec))) {
            *collect_ec = current_ec;
        }
    } else {
        /* always retain last exit code in no-hard-fail mode */
        *collect_ec = current_ec;
    }
}

int main(int argc, char* argv[], char* envp[]) {
    size_t len;
    char line[MAXSTR];
    int other, exitstatus, status = 0;
    int slot, cpus, fail_hard = 0, old_mode = 0;
    char* cmd;
    char* save = NULL;
    unsigned long total = 0;
    unsigned long failure = 0;
    unsigned long lineno = 0;
    unsigned long extra = 0;
    time_t when;
    Jobs jobs;
    double diff, start = now(&when);
    parseCommandline(argc, argv, &fail_hard, &old_mode, &cpus);

    /* progress report finish */
    if (progress != -1) {
        report(progress, start, 0.0, -1, argv, NULL, NULL, 0ul);
    }

    /* allocate job management memory */
    if (jobs_init(&jobs, cpus) == -1) {
        showerr("%s: out of memory: %d: %s\n",
                application, errno, strerror(errno));
        return 42;
    }

    /* since we will create multiple concurrent processes, let's create a
     * process group to order them by.
     */
    if (setpgid(0, 0) == -1) {
        showerr("%s: unable to become process group leader: %d: %s (ignoring)\n",
                application, errno, strerror(errno));
    }

    /* NEW: unconditionally run a setup job */
    run_independent_task(getenv("SEQEXEC_SETUP"), envp, &extra, "setup");

    /* Read the commands and call each sequentially */
    while (fgets(line, sizeof(line), stdin) != NULL) {
        ++lineno;

        /* check for skippable line */
        if (line[0] == 0 || /* empty line */
            line[0] == '\r' || /* CR */
            line[0] == '\n' || /* LF */
            line[0] == '#' /* comment */) {
            continue;
        }

        /* check for unterminated line (line larger than buffer) */
        len = strlen(line);
        if (line[len-1] != '\r' && line[len-1] != '\n') {
            /* read buffer was too small, save and append */
            char* temp = merge(save, line, 0);
            if (save != NULL) free(save);
            save = temp;

            lineno--;
            showerr("%s: continuation line %lu\n", application, lineno);
            continue;
        } else {
            /* remove line termination character(s) */
            do {
                line[len-1] = 0;
                len--;
            } while (len > 0 && (line[len-1] == '\r' || line[len-1] == '\n'));
        }

        /* Assemble command.
         * FIXME: barf if commandline becomes too long, see _SC_ARG_MAX.
         */
        if (save != NULL) {
            cmd = merge(save, line, 0);
            free(save);
            save = NULL;
        } else {
            cmd = line;
        }

        /* find a free slot */
        while ((slot = jobs_first_slot(&jobs, EMPTY)) == jobs.cpus) {
            /* wait for any child to finish */
            if (debug) {
                showerr("%s: %d slot%s busy, wait()ing\n",
                        application, jobs.cpus, (jobs.cpus == 1 ? "" : "s"));
            }
            wait_for_child(&jobs, &other);
            if (errno == 0 && isafailure(other)) {
                failure++;
            }
            massage_failure(fail_hard, other, &status);
        }

        /* post-condition: there is a free slot; slot number in "slot" */

        /* found free slot */
        if (fail_hard && status && isafailure(status)) {
            /* we are in failure mode already, skip starting new stuff */
        } else if (slot < jobs.cpus) {
            /* there is a free slot. Spawn and continue */
            Signals save;
            Job* j = jobs.jobs + slot;
            if ((j->argc = interpretArguments(cmd, &(j->argv))) > 0) {
                /* determine full path to application according to PATH */
                char* fqpn = find_executable(j->argv[0]);
                if (fqpn) {
                    /* found a FQPN, exchange first item in argument vector */
                    free(j->argv[0]);
                    j->argv[0] = fqpn;
                }

                total++;
                j->envp = envp;
                j->lineno = lineno;

                /* WARNING: Must propagate "save" to start_child() */
                save_signals(&save);

                if ((j->child = fork()) == ((pid_t) -1)) {
                    /* fork error, bad */
                    showerr("%s: fork: %d: %s\n",
                            application, errno, strerror(errno));
                    failure++;
                    job_done(j);
                } else if (j->child == ((pid_t) 0)) {
                    /* child code */
                    start_child(j->argv, j->envp, &save);
                    return 127; /* never reached, just in case */
                } else {
                    /* parent code */
                    j->count = total;
                    j->state = RUNNING;
                    j->start = now(&(j->when));
                }

                restore_signals(&save);
            } else {
                /* error parsing args */
                if (debug) {
                    showerr("%s: error parsing arguments on line %lu, ignoring\n",
                            application, lineno);
                }
            }
        } else {
            /* no free slots, wait for children to finish */
            showerr("%s: %s:%d THIS SHOULD NOT HAPPEN! (ignoring)\n",
                    application, __FILE__, __LINE__ );
        }

        if (cmd != line) {
            free(cmd);
        }

        /* fail hard mode, if requested */
        if (fail_hard && status && isafailure(status)) {
            break;
        }
    }

    /* wait for all children */
    while ((slot = jobs_in_state(&jobs, EMPTY)) < jobs.cpus) {
        /* wait for any child to finish */
        size_t n = jobs.cpus - slot;
        if (debug) {
            showerr("%s: %d task%s remaining\n", application, n, (n == 1 ? "" : "s"));
        }
        wait_for_child(&jobs, &other);
        if (errno == 0 && isafailure(other)) {
            failure++;
        }
        massage_failure(fail_hard, other, &status);
    }

    /* NEW: unconditionally run a clean-up job */
    run_independent_task(getenv("SEQEXEC_CLEANUP"), envp, &extra, "cleanup");

    /* compute if seqexec should return any form of failure */
    if (old_mode) {
        exitstatus = 0;
    } else if (fail_hard) {
        exitstatus = (status && isafailure(status)) ? 5 : 0;
    } else {
        exitstatus = failure ? 5 : 0;
    }

    /* provide final statistics */
    jobs_done(&jobs);
    diff = now(NULL) - start;
    showout("[cluster-summary stat=\"%s\", lines=%lu, tasks=%lu, succeeded=%lu, failed=%lu, "
            "extra=%lu, duration=%.3f, start=\"%s\", pid=%d, app=\"%s\"]\n",
            exitstatus ? "fail" : "ok",
            lineno, total, total-failure, failure, extra,
            diff, iso2date(start, line, sizeof(line)),
            getpid(), argv[0]);

    fflush(stdout); /* just in case */
    exit(exitstatus);
}


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
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
#include <ctype.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#include "parse.h"
#include "utils.h"
#include "error.h"

/* In Linux, 32 pages is the max for a single argument.
 * In Darwin it is larger, but we will just use the same
 * value for simplicity. Since we don't limit the number
 * of arguments we could still exceed the max, but then
 * execve will fail with a nice error.
 */
#define KS_ARG_MAX 131072

size_t countNodes(const Node* head) {
    /* purpose: count the number of element in list
     * paramtr: head (IN): start of the list.
     * returns: number of elements in list. */
    const Node* temp;
    size_t result = 0;
    for (temp = head; temp; temp = temp->next) result++;
    return result;
}

void deleteNodes(Node* head) {
    /* purpose: clean up the created list and free its memory.
     * paramtr: head (IO): start of the list. */
    Node* temp;
    while ((temp=head)) {
        head = head->next;
        free((void*) temp->data);
        free((void*) temp);
    }
}


static void add(Node** head, Node** tail, const char* data) {
    /* purpose: add an data item to the end of the list
     * paramtr: head (OUT): head of the list, NULL for no list
     *          tail (OUT): tail of the list, will be adjusted
     *          data (IN): string to save the pointer to (shallow copy)
     */
    Node *n = (Node*) malloc(sizeof(Node));
    if (n == NULL) {
        printerr("malloc: %s\n", strerror(errno));
        exit(1);
    }
    n->data = data;
    n->next = NULL;

    if (*head == NULL) {
        *head = *tail = n;
    } else {
        (*tail)->next = n;
        *tail = n;
    }
}

static void resolve(char** v, char* varname, char** p, char* buffer, size_t size) {
    /* purpose: lookup the variable name in the environment, and
     *          copy the environment value into the buffer
     * paramtr: v (IO): final position of the variable name buffer
     *          varname (IN): start of variable name buffer
     *          p (IO): cursor position of output buffer
     *          buffer: (IO): start of output buffer
     *          size (IN): size of output buffer
     */
    char* value = 0;

    **v = 0;
    if ((value = getenv(varname))) {
        char* pp = *p;
        while (pp - buffer < size && *value) *pp++ = *value++;
        *p = pp;
    } else {
        printerr("ERROR: Variable $%s does not exist\n", varname);
        exit(1);
    }

    *v = varname;
}

/* Parsing pre- and postjob argument line splits whitespaces in shell fashion.
 * state transition table maps from start state and input character to
 * new state and action. The actions are abbreviated as follows:
 *
 * abb | # | meaning
 * ----+---+--------------------------------------------------------
 *  Sb | 0 | store input char into argument buffer
 *  Fb | 1 | flush regular buffer and reset argument buffer pointers
 *  Sv | 2 | store input char into variable name buffer
 *  Fv | 3 | flush varname via lookup into argument buffer and reset vpointers
 * Fvb | 4 | Do Fv followed by Fb
 *  -  | 5 | skip (ignore) input char (do nothing)
 *  *  | 6 | translate abfnrtv to controls, other store verbatim
 *  FS | 7 | Do Fv followed by Sb
 *     | 8 | print error and exit
 *
 * special final states:
 *
 * state | meaning
 * ------+-----------------
 * F  32 | final, leave machine
 * E1 33 | error 1: missing closing apostrophe
 * E2 34 | error 2: missing closing quote
 * E3 35 | error 3: illegal variable name
 * E4 36 | error 4: missing closing brace
 * E5 37 | error 5: premature end of string
 *
 *
 * STATE |  eos |   "  |   '  |   {  |   }  |   $  |   \  | alnum| wspc | else |
 * ------+------+------+------+------+------+------+------+------+------+------+
 *     0 | F,-  | 4,-  | 2,-  | 1,Sb | 1,Sb | 11,- | 14,- | 1,Sb | 0,-  | 1,Sb |
 *     1 | F,Fb | 4,-  | 2,-  | 1,Sb | 1,Sb | 11,- | 14,- | 1,Sb | 0,Fb | 1,Sb |
 *     2 | E1   | 2,Sb | 1,-  | 2,Sb | 2,Sb | 2,Sb | 3,-  | 2,Sb | 2,Sb | 2,Sb |
 *     3 | E1   | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb |
 *     4 | E2   | 1,-  | 4,Sb | 4,Sb | 4,Sb | 8,-  | 7,-  | 4,Sb | 4,Sb | 4,Sb |
 *     7 | E2   | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,*  | 4,Sb | 4,Sb |
 *     8 | E2   | E2   | E2   | 9,-  | E3   | E3   | E3   |10,Sv | E3   | E3   |
 *     9 | E4   | E4   | E4   | E4   | 4,Fv | E3   | 9,Sv | 9,Sv | 9,Sv | 9,Sv |
 *    10 | E2   | 1,Fv | 4,Fv | 4,Fv | 4,Fv | 8,Fv | 4,Fv |10,Sv | 4,Fv |10,Sv |
 *    11 | E3   | E3   | E3   |12,-  | E3   | E3   | E3   |13,Sv | E3   | E3   |
 *    12 | E4   | E4   | E4   | E4   | 1,Fv | E3   |12,Sv |12,Sv |12,Sv |12,Sv |
 *    13 | F,Fvb| 4,Fv | 2,Fv | 1,Fv | 1,Fv | E3   |13,Sv |13,Sv | 1,Fv | 1,FS |
 *    14 | E5   | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb | 1,Sb |
 *
 * '" REMOVED:
 *     5 | E1   | 5,Sb | 4,-  | 5,Sb | 5,Sb | 5,Sb | 6,-  | 5,Sb | 5,Sb | 5,Sb |
 *     6 | E1   | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb |
 */

typedef const char Row[10];
typedef const Row Map[15];

static Map actionmap1 = {
    { 5, 5, 5, 0, 0, 5, 5, 0, 5, 0 }, /*  0 */
    { 1, 5, 5, 0, 0, 5, 5, 0, 1, 0 }, /*  1 */
    { 8, 0, 5, 0, 0, 0, 5, 0, 0, 0 }, /*  2 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  3 */
    { 8, 5, 4, 0, 0, 5, 5, 0, 0, 0 }, /*  4 */
    { 8, 0, 5, 0, 0, 0, 5, 0, 0, 0 }, /*  5 (unused) */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  6 (unused) */
    { 8, 0, 0, 0, 0, 0, 0, 6, 0, 0 }, /*  7 */
    { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, /*  8 */
    { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, /*  9 */
    { 8, 3, 3, 3, 3, 3, 3, 2, 3, 2 }, /* 10 */
    { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, /* 11 */
    { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, /* 12 */
    { 4, 3, 3, 3, 3, 8, 2, 2, 3, 7 }, /* 13 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }  /* 14 */
};

static Map statemap1 = {
    { 32,  4,  2,  1,  1, 11, 14,  1,  0,  1 }, /*  0 */
    { 32,  4,  2,  1,  1, 11, 14,  1,  0,  1 }, /*  1 */
    { 33,  2,  1,  2,  2,  2,  3,  2,  2,  2 }, /*  2 */
    { 33,  2,  2,  2,  2,  2,  2,  2,  2,  2 }, /*  3 */
    { 34,  1,  0,  4,  4,  8,  7,  4,  4,  4 }, /*  4 */
    { 33,  5,  4,  5,  5,  5,  6,  5,  5,  5 }, /*  5 (unused) */
    { 33,  5,  5,  5,  5,  5,  5,  5,  5,  5 }, /*  6 (unused) */
    { 34,  4,  4,  4,  4,  4,  4,  4,  4,  5 }, /*  7 */
    { 34, 34, 34,  9, 34, 34, 34, 10, 34, 34 }, /*  8 */
    { 36, 36, 36, 36,  4, 36,  9,  9,  9,  9 }, /*  9 */
    { 34,  1,  4,  4,  4,  8,  4, 10,  4, 10 }, /* 10 */
    { 35, 35, 35, 12, 35, 35, 35, 13, 35, 35 }, /* 11 */
    { 36, 36, 36, 36,  1, 35, 12, 12, 12, 12 }, /* 12 */
    { 32,  4,  2,  1,  1, 35, 13, 13,  1,  1 }, /* 13 */
    { 37,  1,  1,  1,  1,  1,  1,  1,  1,  1 }  /* 14 */
};

static const char* errormessage[5] = {
    "Error 1: missing closing apostrophe\n",
    "Error 2: missing closing quote\n",
    "Error 3: illegal variable name\n",
    "Error 4: missing closing brace\n",
    "Error 5: premature end of string\n"
};

static const char* translation = "abnrtv";
static const char translationmap[] = "\a\b\n\r\t\v";


/* Parsing main job argument vector maintains whitespace.
 * state transition table maps from start state and input character to
 * new state and action. The actions are abbreviated as specified above.
 *
 * STATE |  eos |   "  |   '  |   {  |   }  |   $  |   \  | alnum| wspc | else |
 * ------+------+------+------+------+------+------+------+------+------+------+
 *     0 | F,Fb | 4,Sb | 2,Sb | 0,Sb | 0,Sb | 11,- | 1,-  | 0,Sb | 0,Sb | 0,Sb |
 *     1 | E5   | 0,Sb | 0,Sb | 0,Sb | 0,Sb | 0,Sb | 0,Sb | 0,Sb | 0,Sb | 0,Sb |
 *     2 | E1   | 2,Sb | 0,Sb | 2,Sb | 2,Sb | 2,Sb | 3,Sb | 2,Sb | 2,Sb | 2,Sb |
 *     3 | E1   | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb | 2,Sb |
 *     4 | E2   | 0,Sb | 4,Sb | 4,Sb | 4,Sb | 8,-  | 7,Sb | 4,Sb | 4,Sb | 4,Sb |
 *     7 | E2   | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb | 4,Sb |
 *     8 | E2   | E2   | E2   | 9,-  | E3   | E3   | E3   |10,Sv | E3   | E3   |
 *     9 | E4   | E4   | E4   | E4   | 4,Fv | E3   | 9,Sv | 9,Sv | 9,Sv | 9,Sv |
 *    10 | E2   | 0,FS | 4,FS | 4,FS | 4,FS | 8,Fv | 4,FS |10,Sv | 4,FS |10,Sv |
 *    11 | E3   | E3   | E3   |12,-  | E3   | E3   | E3   |13,Sv | E3   | E3   |
 *    12 | E4   | E4   | E4   | E4   | 0,Fv | E3   |12,Sv |12,Sv |12,Sv |12,Sv |
 *    13 | F,Fvb| 4,FS | 2,FS | 0,FS | 0,FS | E3   |13,Sv |13,Sv | 0,FS |0,FS  |
 *
 * '" REMOVED
 *     5 | E1   | 5,Sb | 4,Sb | 5,Sb | 5,Sb | 5,Sb | 6,Sb | 5,Sb | 5,Sb | 5,Sb |
 *     6 | E1   | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb | 5,Sb |
 */
static Map actionmap2 = {
    { 1, 0, 0, 0, 0, 5, 5, 0, 0, 0 }, /*  0 FIXED: \\ 0 -> 5 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  1 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  2 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  3 */
    { 8, 0, 0, 0, 0, 5, 0, 0, 0, 0 }, /*  4 */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  5 (unused) */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  6 (unused) */
    { 8, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, /*  7 */
    { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, /*  8 */
    { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, /*  9 */
    { 8, 7, 7, 7, 7, 3, 7, 2, 7, 2 }, /* 10 */
    { 8, 8, 8, 5, 8, 8, 8, 2, 8, 8 }, /* 11 */
    { 8, 8, 8, 8, 3, 8, 2, 2, 2, 2 }, /* 12 */
    { 4, 7, 7, 7, 7, 8, 2, 2, 7, 7 }, /* 13 */
    { 8, 8, 8, 8, 8, 8, 8, 8, 8, 8 }  /* unused */
};

static Map statemap2 = {
    { 32,  4,  2,  0,  0, 11,  1,  0,  0,  0 }, /*  0 */
    { 37,  0,  0,  0,  0,  0,  0,  0,  0,  0 }, /*  1 */
    { 33,  2,  0,  2,  2,  2,  3,  2,  2,  2 }, /*  2 */
    { 33,  2,  2,  2,  2,  2,  2,  2,  2,  2 }, /*  3 */
    { 34,  0,  4,  4,  4,  8,  7,  4,  4,  4 }, /*  4 */
    { 33,  5,  4,  5,  5,  5,  6,  5,  5,  5 }, /*  5 (unused) */
    { 33,  5,  5,  5,  5,  5,  5,  5,  5,  5 }, /*  6 (unused) */
    { 34,  4,  4,  4,  4,  4,  4,  4,  4,  4 }, /*  7 */
    { 34, 34, 34,  9, 35, 35, 35, 10, 35, 35 }, /*  8 */
    { 36, 36, 36, 36,  4, 35,  9,  9,  9,  9 }, /*  9 */
    { 34,  0,  4,  4,  4,  8,  4, 10,  4, 10 }, /* 10 */
    { 35, 35, 35, 12, 35, 35, 35, 13, 35, 35 }, /* 11 */
    { 36, 36, 36, 36,  0, 35, 12, 12, 12, 12 }, /* 12 */
    { 32,  4,  2,  0,  0, 35, 13, 13,  0,  0 }, /* 13 */
    { 32, 32, 32, 32, 32, 32, 32, 32, 32, 32 }  /* unused */
};

static int xlate(char input) {
    /* purpose: translate an input character into the character class.
     * paramtr: input (IN): input character
     * returns: numerical character class for input character.
     */
    switch (input) {
        case 0:
            return 0;
        case '\"': /* " */
            return 1;
        case '\'': /* ' */
            return 2;
        case '{':
            return 3;
        case '}':
            return 4;
        case '$':
            return 5;
        case '\\':
            return 6;
        default:
            if (isalnum(input) || input=='_') {
                return 7;
            } else if (isspace(input)) {
                return 8;
            } else {
                return 9;
            }
    }
}

static void internalParse(const char *line, Map actionmap, Map statemap,
                          Node **headp, Node **tailp, char *buffer, size_t size) {
    int state = 0;
    const char *s = line;
    char *p = buffer;
    char varname[128];
    size_t vsize = sizeof(varname);
    char *v = varname;
    Node *head = *headp;
    Node *tail = *tailp;
    char *data;

    while (state < 32) {
        int charclass = xlate(*s);
        int newstate = statemap[state][charclass];

        switch (actionmap[state][charclass]) {
            case 0: /* store into buffer */
                if (p-buffer < size) {
                    *p = *s;
                    p++;
                } else {
                    printerr("ERROR: Argument too long\n");
                    exit(1);
                }
                break;
            case 1: /* conditionally finalize buffer */
                *p = '\0';
                data = strdup(buffer);
                if (data == NULL) {
                    printerr("strdup: %s\n", strerror(errno));
                    exit(1);
                }
                add(&head, &tail, data);
                p = buffer;
                break;
            case 2: /* store variable part */
                if (v-varname < vsize) {
                    *v++ = *s;
                } else {
                    printerr("ERROR: Variable name too long\n");
                    exit(1);
                }
                break;
            case 3: /* finalize variable name */
                resolve(&v, varname, &p, buffer, size);
                break;
            case 4: /* case 3 followed by case 1 */
                resolve(&v, varname, &p, buffer, size);
                *p = '\0';
                data = strdup(buffer);
                if (data == NULL) {
                    printerr("strdup: %s\n", strerror(errno));
                    exit(1);
                }
                add(&head, &tail, data);
                p = buffer;
                break;
            case 5: /* skip */
                break;
            case 6: /* translate control escapes */
                if (p-buffer < size) {
                    char* x = strchr(translation, *s);
                    *p++ = (x == NULL ? *s : translationmap[x-translation]);
                }
                break;
            case 7: /* case 3 followed by case 0 */
                resolve(&v, varname, &p, buffer, size);
                if (p - buffer < size) *p++ = *s;
                break;
            case 8: /* print error message */
                if (newstate > 32) {
                    printerr("Error parsing arguments: %s", errormessage[newstate-33]);
                } else {
                    printerr("Error parsing arugments: state=%02d, class=%d, "
                             "action=%d, newstate=%02d, char=%02X (%c)\n",
                             state, charclass, 8, newstate, *s, 
                             ((*s & 127) >= 32) ? *s : '.');
                }
                exit(1);
                break;
        }
        ++s;
        state = newstate;
    }

    /* update various cursors */
    *tailp = tail;
    *headp = head;
}

Node *parseCommandLine(const char* line) {
    /* purpose: parse a commandline into a list of arguments while
     *          obeying single quotes, double quotes and replacing
     *          environment variable names.
     * paramtr: line (IN): commandline to parse
     * returns: A list of split arguments or NULL on error
     */
    if (line == NULL) {
        return NULL;
    }

    Node* head = NULL;
    Node* tail = NULL;

    size_t size = KS_ARG_MAX;
    char* buffer = malloc(size);
    if (buffer == NULL) {
        printerr("malloc: %s\n", strerror(errno));
        exit(1);
    }

    internalParse(line, actionmap1, statemap1, &head, &tail,
                  buffer, size);

    free(buffer);

    return head;
}

Node *parseArgVector(int argc, char* const* argv) {
    /* purpose: parse an already split commandline into a list of arguments while
     *          ONLY translating environment variable names that are not prohibited
     *          from translation by some form of quoting (not double quotes, though).
     * paramtr: argc (IN): number of arguments in the argument vector
     *          argv (IN): argument vector to parse
     * returns: A list of split arguments or NULL if there was an error. The
     *          argument number stays the same, but environment variables are
     *          translated.
     */
    if (argc == 0) {
        return NULL;
    }

    Node* head = NULL;
    Node* tail = NULL;

    size_t size = KS_ARG_MAX;
    char* buffer = malloc(size);
    if (buffer == NULL) {
        printerr("malloc: %s\n", strerror(errno));
        exit(1);
    }

    /* invoke parsing once for each argument */
    for (int i=0; i<argc; ++i) {
        internalParse(argv[i], actionmap2, statemap2, &head, &tail,
                      buffer, size);
    }

    free(buffer);

    return head;
}


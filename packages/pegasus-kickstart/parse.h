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
#ifndef _PARSE_H
#define _PARSE_H

typedef struct _Node {
    const char*   data;
    struct _Node* next;
} Node;

extern size_t countNodes(const Node* head);
extern void deleteNodes(Node* head);
extern Node *parseCommandLine(const char* line);
extern Node *parseArgVector(int argc, char* const* argv);

#endif /* _PARSE_H */

/**
 *  Copyright 2007-2010 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#ifndef _TOOLS_H
#define _TOOLS_H

#include <sys/types.h>
#include <stdarg.h>
#include <time.h>
#include <sys/time.h>

#ifndef MAXSTR
#define MAXSTR 4096
#endif

extern
ssize_t
writen( int fd, const char* buffer, ssize_t n, unsigned restart );
/* purpose: write all n bytes in buffer, if possible at all
 * paramtr: fd (IN): filedescriptor open for writing
 *          buffer (IN): bytes to write (must be at least n byte long)
 *          n (IN): number of bytes to write
 *          restart (IN): if true, try to restart write at max that often
 * returns: n, if everything was written, or
 *          [0..n-1], if some bytes were written, but then failed,
 *          < 0, if some error occurred.
 */

extern
ssize_t
showerr( const char* fmt, ... );
/* purpose: printf-like error using (hopefully) atomic writes
 * paramtr: see printf()
 * returns: number of bytes written, -1 for error
 */ 

/* as long as showerr() reports on stdout, we can use this macro */ 
#ifndef showout
#define showout showerr
#endif

extern
double
timespec( struct timeval* tv );
/* purpose: convert a timeval into float seconds with microseconds.
 * paramtr: tv (IN): pointer to struct timeval to convert
 * returns: seconds as float with microseconds.
 * warning: anything below millisecond resolution is unrealistic. 
 */

extern
double
now( time_t* when );
/* purpose: obtains an UTC timestamp with microsecond resolution.
 * paramtr: when (opt. OUT): where to save integral seconds into. 
 * returns: the timestamp, or -1.0 if it was completely impossible.
 */

extern
char*
isodate( time_t seconds, char* buffer, size_t size );
/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds (IN): time stamp
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */

extern
char*
iso2date( double seconds_wf, char* buffer, size_t size );
/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds_wf (IN): time stamp with fractional seconds (millis)
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */

#endif /* _TOOLS_H */

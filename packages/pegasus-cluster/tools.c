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
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "tools.h"

ssize_t
writen( int fd, const char* buffer, ssize_t n, unsigned restart )
/* purpose: write all n bytes in buffer, if possible at all
 * paramtr: fd (IN): filedescriptor open for writing
 *          buffer (IN): bytes to write (must be at least n byte long)
 *          n (IN): number of bytes to write
 *          restart (IN): if true, try to restart write at max that often
 * returns: n, if everything was written, or
 *          [0..n-1], if some bytes were written, but then failed,
 *          < 0, if some error occurred.
 */
{
  int start = 0;
  while ( start < n ) {
    int size = write( fd, buffer+start, n-start );
    if ( size < 0 ) {
      if ( restart && errno == EINTR ) { restart--; continue; }
      return size;
    } else {
      start += size;
    }
  }
  return n;
}

ssize_t
showerr( const char* fmt, ... )
{
  char line[MAXSTR];
  va_list ap;

  va_start( ap, fmt );
  vsnprintf( line, sizeof(line), fmt, ap );
  va_end(ap);

  /* (almost) atomic write */
  return writen( STDOUT_FILENO, line, strlen(line), 3 );
}

double 
timespec( struct timeval* tv )
{
  return ( tv->tv_sec + tv->tv_usec / 1E6 ); 
}

double
now( time_t* when )
/* purpose: obtains an UTC timestamp with microsecond resolution.
 * paramtr: when (opt. OUT): where to save integral seconds into. 
 * returns: the timestamp, or -1.0 if it was completely impossible.
 */
{
  int timeout = 0;
  struct timeval t = { -1, 0 };
  while ( gettimeofday( &t, NULL ) == -1 && timeout < 10 ) timeout++;
  if ( when != NULL ) *when = t.tv_sec;
  return timespec(&t); 
}

char*
isodate( time_t seconds, char* buffer, size_t size )
/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds (IN): time stamp
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
{
  struct tm zulu = *gmtime(&seconds);
  struct tm local = *localtime(&seconds);
  zulu.tm_isdst = local.tm_isdst;
  {
    time_t distance = (seconds - mktime(&zulu)) / 60;
    int hours = distance / 60;
    int minutes = distance < 0 ? -distance % 60 : distance % 60;
    size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
    snprintf( buffer+len, size-len, "%+03d:%02d", hours, minutes );
  }
  return buffer;
}

char*
iso2date( double seconds_wf, char* buffer, size_t size )
/* purpose: formats ISO 8601 timestamp into given buffer (simplified)
 * paramtr: seconds_wf (IN): time stamp with fractional seconds (millis)
 *          buffer (OUT): where to put the results
 *          size (IN): capacity of buffer
 * returns: pointer to start of buffer for convenience. 
 */
{
  char millis[8]; 
  double integral, fractional = modf(seconds_wf,&integral); 
  time_t seconds = (time_t) integral; 
  struct tm zulu = *gmtime(&seconds);
  struct tm local = *localtime(&seconds);
  zulu.tm_isdst = local.tm_isdst;
  snprintf( millis, sizeof(millis), "%.3f", fractional ); 
  {
    time_t distance = (seconds - mktime(&zulu)) / 60;
    int hours = distance / 60;
    int minutes = distance < 0 ? -distance % 60 : distance % 60;
    size_t len = strftime( buffer, size, "%Y-%m-%dT%H:%M:%S", &local );
    snprintf( buffer+len, size-len, "%s%+03d:%02d", millis+1, hours, minutes );
  }
  return buffer;
}

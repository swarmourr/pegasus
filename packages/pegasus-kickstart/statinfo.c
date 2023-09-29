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
#include <sys/param.h>
#include <limits.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <wchar.h>
#include <locale.h>

#include <unistd.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>

#include "statinfo.h"
#include "utils.h"
#include "checksum.h"
#include "error.h"

size_t data_section_size = 262144ul;

int forcefd(const StatInfo* info, int fd) {
    /* purpose: force open a file on a certain fd
     * paramtr: info (IN): is the StatInfo of the file to connect to (fn or fd)
     *          the mode for potential open() is determined from this, too.
     *          fd (IN): is the file descriptor to plug onto. If this fd is
     *          the same as the descriptor in info, nothing will be done.
     * returns: 0 if all is well, or fn was NULL or empty.
     *          1 if opening a filename failed,
     *          2 if dup2 call failed
     */
    /* is this a regular file with name, or is this a descriptor to copy from? */
    int isHandle = (info->source == IS_HANDLE || info->source == IS_TEMP);
    int mode = info->file.descriptor; /* openmode for IS_FILE */

    /* initialize the newHandle variable by opening regular files, or copying the fd */
    int newfd = isHandle ?
        info->file.descriptor :
        (((mode & O_ACCMODE) == O_RDONLY) ?
          open(info->file.name, mode) :
          /* FIXME: as long as stdout/stderr is shared between jobs,
           * we must always use append mode. Truncation happens during
           * initialization of the shared stdio. */
          open(info->file.name, mode | O_APPEND, 0666));

    /* this should only fail in the open() case */
    if (newfd == -1) {
        return 1;
    }

    /* create a duplicate of the new fd onto the given (stdio) fd. This operation
     * is guaranteed to close the given (stdio) fd first, if open. */
    if (newfd != fd) {
        /* FIXME: Does dup2 guarantee noop for newfd==fd on all platforms ? */
        if (dup2(newfd, fd) == -1) {
            return 2;
        }
    }

    /* if we opened a file, we need to close it again. */
    if (! isHandle) {
        close(newfd);
    }

    return 0;
}

int initStatInfoAsTemp(StatInfo* statinfo, char* pattern) {
    /* purpose: Initialize a stat info buffer with a temporary file
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          pattern (IO): is the input pattern to mkstemp(), will be modified!
     * returns: a value of -1 indicates an error
     */
    memset(statinfo, 0, sizeof(StatInfo));

    int fd = mkstemp(pattern);
    if (fd < 0) {
        printerr("mkstemp: %s\n", strerror(errno));
        goto error;
    }

    char *filename = strdup(pattern);
    if (filename == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        goto error;
    }

    /* try to ensure append mode for the file, because it is shared
     * between jobs. If the SETFL operation fails, well there is nothing
     * we can do about that. */
    int flags = fcntl(fd, F_GETFL);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags | O_APPEND);
    }

    /* this file descriptor is NOT to be passed to the jobs? So far, the
     * answer is true. We close this fd on exec of sub jobs, so it will
     * be invisible to them. */
    flags = fcntl(fd, F_GETFD);
    if (flags != -1) {
        fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
    }

    /* the return is the chosen filename as well as the opened descriptor.
     * we *could* unlink the filename right now, and be truly private, but
     * users may want to look into the log files of long persisting operations. */
    statinfo->source = IS_TEMP;
    statinfo->file.descriptor = fd;
    statinfo->file.name = filename;
    statinfo->error = 0;

    errno = 0;
    int result = fstat(fd, &statinfo->info);
    if (result < 0) {
        printerr("fstat: %s\n", strerror(errno));
        goto error;
    }

    return 0;

error:
    statinfo->source = IS_INVALID;
    statinfo->error = errno;

    return -1;
}

static int preserveFile(const char* fn) {
    /* purpose: preserve the given file by renaming it with a backup extension.
     * paramtr: fn (IN): name of the file
     * returns: 0: ok; -1: error, check errno
     */
    int i, fd = open(fn, O_RDONLY);
    if (fd != -1) {
        /* file exists, do something */
        size_t size = strlen(fn)+8;
        char* newfn = malloc(size);
        if (newfn == NULL) {
            printerr("malloc: %s\n", strerror(errno));
            return -1;
        }

        close(fd);
        strncpy(newfn, fn, size);
        for (i=0; i<1000; ++i) {
            snprintf(newfn + size-8, 8, ".%03d", i);
            if ((fd = open(newfn, O_RDONLY)) == -1) {
                if (errno == ENOENT) break;
                else return -1;
            }
            close(fd);
        }

        if (i < 1000) {
            return rename(fn, newfn);
        } else {
            /* too many backups */
            errno = EEXIST;
            return -1;
        }
    } else {
        /* file does not exist, nothing to backup */
        errno = 0;
        return 0;
    }
}

int initStatInfoFromName(StatInfo* statinfo, const char* filename,
                         int openmode, int flag) {
    /* purpose: Initialize a stat info buffer with a filename to point to
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          filename (IN): the filename to memorize (deep copy)
     *          openmode (IN): are the fcntl O_* flags to later open calls
     *          flag (IN): bit#0 truncate: whether to reset the file size to zero
     *                     bit#1 defer op: whether to defer opening the file for now
     *                     bit#2 preserve: whether to backup existing target file
     * returns: the result of the stat() system call on the provided file */
    int result = -1;
    memset(statinfo, 0, sizeof(StatInfo));
    statinfo->source = IS_FILE;
    statinfo->file.descriptor = openmode;
    statinfo->file.name = strdup(filename);
    if (statinfo->file.name == NULL) {
        printerr("strdup: %s\n", strerror(errno));
        statinfo->error = errno;
        return -1;
    }

    if ((flag & 0x01) == 1) {
        /* FIXME: As long as we use shared stdio for stdout and stderr, we need
         * to explicitely truncate (and create) file to zero, if not appending.
         */
        if ((flag & 0x02) == 0) {
            int fd;
            if ((flag & 0x04) == 4) {
                preserveFile(filename);
            }
            fd = open(filename, (openmode & O_ACCMODE) | O_CREAT | O_TRUNC, 0666);
            if (fd != -1) {
                close(fd);
            }
        } else {
            statinfo->deferred = 1 | (flag & 0x04);
        }
    }
    /* POST-CONDITION: statinfo->deferred == 1, iff (flag & 3) == 3 */

    errno = 0;
    result = stat(filename, &statinfo->info);
    statinfo->error = errno;

    /* special case, read the start of file (for magic) */
    if ((flag & 0x02) == 0 &&
        result != -1 &&
        S_ISREG(statinfo->info.st_mode) &&
        statinfo->info.st_size > 0) {

        int fd = open(filename, O_RDONLY);
        if (fd != -1) {
            read(fd, (char*) statinfo->client.header, sizeof(statinfo->client.header));
            close(fd);
        }
    }

    return result;
}

int updateStatInfo(StatInfo* statinfo) {
    /* purpose: update existing and initialized statinfo with latest info
     * paramtr: statinfo (IO): stat info pointer to update
     * returns: the result of the stat() or fstat() system call. */
    int result = -1;

    if (statinfo->source == IS_FILE && (statinfo->deferred & 1) == 1) {
        /* FIXME: As long as we use shared stdio for stdout and stderr, we need
         * to explicitely truncate (and create) file to zero, if not appending.
         */
        int fd;
        if ((statinfo->deferred & 4) == 4) {
            preserveFile(statinfo->file.name);
        }
        fd = open(statinfo->file.name,
                  (statinfo->file.descriptor & O_ACCMODE) | O_CREAT | O_TRUNC,
                  0666);
        if (fd != -1) {
            close(fd);
        }

        /* once only */
        statinfo->deferred &= ~1;  /* remove deferred bit */
        statinfo->deferred |=  2;  /* mark as having gone here */
    }

    if (statinfo->source == IS_FILE ||
        statinfo->source == IS_HANDLE ||
        statinfo->source == IS_TEMP ||
        statinfo->source == IS_FIFO) {

        errno = 0;
        if (statinfo->source == IS_FILE) {
            result = stat(statinfo->file.name, &(statinfo->info));
        } else {
            result = fstat(statinfo->file.descriptor, &(statinfo->info));
        }
        statinfo->error = errno;

        if (result != -1 &&
            statinfo->source == IS_FILE &&
            S_ISREG(statinfo->info.st_mode) &&
            statinfo->info.st_size > 0) {

            int fd = open(statinfo->file.name, O_RDONLY);
            if (fd != -1) {
                read(fd, (char*) statinfo->client.header, sizeof(statinfo->client.header));
                close(fd);
            }
        }
    }

    return result;
}

int initStatInfoFromHandle(StatInfo* statinfo, int descriptor) {
    /* purpose: Initialize a stat info buffer with a filename to point to
     * paramtr: statinfo (OUT): the newly initialized buffer
     *          descriptor (IN): the handle to attach to
     * returns: the result of the fstat() system call on the provided handle */
    int result = -1;
    memset(statinfo, 0, sizeof(StatInfo));
    statinfo->source = IS_HANDLE;
    statinfo->file.descriptor = descriptor;

    errno = 0;
    result = fstat(descriptor, &statinfo->info);
    statinfo->error = errno;

    return result;
}

int addLFNToStatInfo(StatInfo* info, const char* lfn) {
    /* purpose: optionally replaces the LFN field with the specified LFN
     * paramtr: info (IO): stat info pointer to update
     *          lfn (IN): LFN to store, use NULL to free
     * returns: errno in case of error, 0 if OK.
     */

    /* sanity check */
    if (info->source == IS_INVALID) {
        return EINVAL;
    }

    if (info->lfn != NULL) {
        free((void*) info->lfn);
    }

    if (lfn == NULL) {
        info->lfn = NULL;
    } else {
        info->lfn = strdup(lfn);
        if (info->lfn == NULL) {
            printerr("strdup: %s\n", strerror(errno));
            return ENOMEM;
        }
    }

    return 0;
}

size_t printYAMLStatInfo(FILE *out, int indent, const char* id,
                        const StatInfo* info, int includeData, int useCDATA,
                        int allowTruncate) {
    /* return number of errors encountered - we still want to see the record */
    char *real = NULL;

    /* sanity check */
    if (info->source == IS_INVALID) {
        return 0;
    }

    if (strcmp(id, "initial") == 0 || strcmp(id, "final") == 0) {
        if (info->lfn != NULL) {
            fprintf(out, "%*s%s:\n", indent, "", info->lfn);
        }
        else {
            fprintf(out, "%*s%s:\n", indent, "", info->file.name);
        }
    }
    else {
        fprintf(out, "%*s%s:\n", indent, "", id);
    }

    if (info->error != 0) {
        fprintf(out, "%*serror: %d\n", indent+2, "", info->error);
    }
    if (info->lfn != NULL) {
        fprintf(out, "%*slfn: \"%s\"\n", indent+2, "", info->lfn);
    }

    /* NEW: ignore "file not found" error for "kickstart" */
    if (id != NULL && info->error == 2 && strcmp(id, "kickstart") == 0) {
        fprintf(out, "%*snote: ignore error - it is just a warning\n", indent+2, "");
    }

    /* either a <name> or <descriptor> sub element */
    switch (info->source) {
        case IS_TEMP:   /* preparation for <temporary> element */
            /* late update for temp files */
            errno = 0;
            if (fstat(info->file.descriptor, (struct stat*) &info->info) != -1 &&
                (((StatInfo*) info)->error = errno) == 0) {

                /* obtain header of file */
                int fd = dup(info->file.descriptor);
                if (fd != -1) {
                    if (lseek(fd, 0, SEEK_SET) != -1) {
                        read(fd, (char*) info->client.header, sizeof(info->client.header));
                    }
                    close(fd);
                }
            }

            fprintf(out, "%*stemporary_name: %s\n%*sdescriptor: %d\n",
                    indent+2, "", info->file.name, indent+2, "", info->file.descriptor);
            break;

        case IS_FIFO: /* <fifo> element */
            fprintf(out, "%*sfifo_name: \"%s\"\n%*sdescriptor: %d\n%*scount: %zu\n%*srsize: %zu\n%*swsize: %zu\n",
                    indent+2, "", info->file.name,
                    indent+2, "", info->file.descriptor,
                    indent+2, "", info->client.fifo.count,
                    indent+2, "", info->client.fifo.rsize,
                    indent+2, "", info->client.fifo.wsize);
            break;

        case IS_FILE: /* <file> element */
            real = realpath(info->file.name, NULL);
            fprintf(out, "%*sfile_name: %s\n", indent+2, "", real ? real : info->file.name);
            if (real) {
                free((void*) real);
            }
            break;

        case IS_HANDLE: /* <descriptor> element */
            fprintf(out, "%*sdescriptor_number: %u\n", indent+2, "",
                    info->file.descriptor);
            break;

        default: /* this must not happen! */
            fprintf(out, "%*serror: No valid file info available\n",
                    indent+2, "");
            break;
    }

    if (info->error == 0 && info->source != IS_INVALID) {
        /* <stat> subrecord */
        char my[32];
        struct passwd* user = getpwuid(info->info.st_uid);
        struct group* group = getgrgid(info->info.st_gid);

        fprintf(out, "%*smode: 0o%o\n", indent+2, "", info->info.st_mode);

        /* Grmblftz, are we in 32bit, 64bit LFS on 32bit, or 64bit on 64 */
        sizer(my, sizeof(my), sizeof(info->info.st_size), &info->info.st_size);
        fprintf(out, "%*ssize: %s\n", indent+2, "", my);

        sizer(my, sizeof(my), sizeof(info->info.st_ino), &info->info.st_ino);
        fprintf(out, "%*sinode: %s\n", indent+2, "", my);

        sizer(my, sizeof(my), sizeof(info->info.st_nlink), &info->info.st_nlink);
        fprintf(out, "%*snlink: %s\n", indent+2, "", my);

        sizer(my, sizeof(my), sizeof(info->info.st_blksize), &info->info.st_blksize);
        fprintf(out, "%*sblksize: %s\n", indent+2, "", my);

        /* st_blocks is new in iv-1.8 */
        sizer(my, sizeof(my), sizeof(info->info.st_blocks), &info->info.st_blocks);
        fprintf(out, "%*sblocks: %s\n", indent+2, "", my);

        fprintf(out, "%*smtime: %s\n", indent+2, "", fmtisodate(info->info.st_mtime, -1));
        fprintf(out, "%*satime: %s\n", indent+2, "", fmtisodate(info->info.st_atime, -1));
        fprintf(out, "%*sctime: %s\n", indent+2, "", fmtisodate(info->info.st_ctime, -1));

        fprintf(out, "%*suid: %d\n", indent+2, "", info->info.st_uid);
        if (user) {
            fprintf(out, "%*suser: %s\n", indent+2, "", user->pw_name);
        }
        fprintf(out, "%*sgid: %d\n", indent+2, "", info->info.st_gid);
        if (group) {
            fprintf(out, "%*sgroup: %s\n", indent+2, "", group->gr_name);
        }
    }

    /* checksum the files if the checksum tools are available
     * and it is a "final" entry
     */
    if (id != NULL && info->error == 0 && strcmp(id, "final") == 0) {
         fprintf(out, "%*soutput: True\n", indent+2, "");
        size_t result = 0;
        char chksum_xml[2048];
        real = realpath(info->file.name, NULL);
        result = pegasus_integrity_yaml(real, chksum_xml);
        if (result == 1) {
            fprintf(out, "%s", chksum_xml);
        }
        else {
            fprintf(out, "%*sintegrity_error: failed creating a checksum\n", indent+2, "");
            return 1;
        }
        if (real) {
            free((void*) real);
        }
    }

    /* if truncation is allowed, then the maximum amount of
     * data that can be put into the invocation record is
     * data_section_size, otherwise add the whole file
     */
    size_t fsize = info->info.st_size;
    size_t dsize = fsize;
    if (allowTruncate) {
        dsize = data_section_size;
    }

    /* data section from stdout and stderr of application */
    if (includeData &&
        info->source == IS_TEMP &&
        info->error == 0 &&
        fsize > 0 && dsize > 0) {

        fprintf(out, "%*sdata_truncated: %s\n", indent+2, "",
                (fsize > dsize ? "true" : "false"));
        fprintf(out, "%*sdata: |\n", indent+2, "");
        if (fsize > 0) {
            /* initial indent */
            fprintf(out, "%*s", indent+4, "");

            wint_t c;
            size_t ccount = 0;
            size_t cskip = 0;
            int fd = dup(info->file.descriptor);
            if (fd != -1) {
                /* as utf8 can be multibyte, we have to walk the file twice - once
                * to figure out how many characters to skip, and once to output */
                if (lseek(fd, 0, SEEK_SET) != -1) {
                    FILE *in = fdopen(fd, "r");
                    while ((c = fgetwc(in)) != WEOF)
                        ccount++;
                    if (ccount > dsize)
                        cskip = ccount - dsize;
                    /* reset and start skipping */
                    ccount = 0;
                    lseek(fd, 0, SEEK_SET);
                    in = fdopen(fd, "r");
                    while (ccount < cskip && (c = fgetwc(in)) != WEOF) 
                        ccount++;
                    /* the rest of the file can be dumped to the yaml output */
                    yamldump(in, out, indent+4);
                }
                close(fd);
            }
            /* final newline */
            fprintf(out, "\n");
        }
    }

    return 0;
}

void deleteStatInfo(StatInfo* statinfo) {
    /* purpose: clean up and invalidates structure after being done.
     * paramtr: statinfo (IO): clean up record. */

    if (statinfo->source == IS_FILE ||
        statinfo->source == IS_TEMP ||
        statinfo->source == IS_FIFO) {

        if (statinfo->source == IS_TEMP || statinfo->source == IS_FIFO) {
            close(statinfo->file.descriptor);
            unlink(statinfo->file.name);
        }

        if (statinfo->file.name) {
            free((void*) statinfo->file.name);
            statinfo->file.name = NULL; /* avoid double free */
        }
    }

    /* invalidate */
    statinfo->source = IS_INVALID;
}


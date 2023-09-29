{"read",		handle_read}, 	/*0*/
{"write",		handle_write},	/*1*/
{"open",		handle_open}, 	/*2*/
{"close",		handle_close}, 	/*3*/
{"stat",		handle_none}, 	/*4*/
{"fstat",		handle_none}, 	/*5*/
{"lstat",		handle_none}, 	/*6*/
{"poll",		handle_none}, 	/*7*/
{"lseek",		handle_none}, 	/*8*/
{"mmap",		handle_none}, 	/*9*/
{"mprotect",	handle_none}, 	/*10*/
{"munmap",		handle_none}, 	/*11*/
{"brk",			handle_none}, 	/*12*/
{"rt_sigaction",	handle_none}, 	/*13*/
{"rt_sigprocmask",	handle_none}, 	/*14*/
{"rt_sigreturn",	handle_none}, 	/*15*/
{"ioctl",		handle_none}, 	/*16*/
{"pread64",		handle_read}, 	/*17*/
{"pwrite64",	handle_write}, 	/*18*/
{"readv",		handle_read}, 	/*19*/
{"writev",		handle_write}, 	/*20*/
{"access",		handle_none}, 	/*21*/
{"pipe",		handle_none}, 	/*22*/
{"select",		handle_none}, 	/*23*/
{"sched_yield",	handle_none}, 	/*24*/
{"mremap",		handle_none}, 	/*25*/
{"msync",		handle_none}, 	/*26*/
{"mincore",		handle_none}, 	/*27*/
{"madvise",		handle_none}, 	/*28*/
{"shmget",		handle_none}, 	/*29*/
{"shmat",		handle_none}, 	/*30*/
{"shmctl",		handle_none}, 	/*31*/
{"dup",			handle_dup}, 	/*32*/
{"dup2",		handle_dup2}, 	/*33*/
{"pause",		handle_none}, 	/*34*/
{"nanosleep",		handle_none}, 	/*35*/
{"getitimer",		handle_none}, 	/*36*/
{"alarm",		handle_none}, 	/*37*/
{"setitimer",		handle_none}, 	/*38*/
{"getpid",		handle_none}, 	/*39*/
{"sendfile",		handle_none}, 	/*40*/
{"socket",		handle_none}, 	/*41*/
{"connect",		handle_none}, 	/*42*/
{"accept",		handle_none}, 	/*43*/
{"sendto",		handle_none}, 	/*44*/
{"recvfrom",		handle_none}, 	/*45*/
{"sendmsg",		handle_none}, 	/*46*/
{"recvmsg",		handle_none}, 	/*47*/
{"shutdown",		handle_none}, 	/*48*/
{"bind",		handle_none}, 	/*49*/
{"listen",		handle_none}, 	/*50*/
{"getsockname",		handle_none}, 	/*51*/
{"getpeername",		handle_none}, 	/*52*/
{"socketpair",		handle_none}, 	/*53*/
{"setsockopt",		handle_none}, 	/*54*/
{"getsockopt",		handle_none}, 	/*55*/
{"clone",		handle_none}, 	/*56*/
{"fork",		handle_none}, 	/*57*/
{"vfork",		handle_none}, 	/*58*/
{"execve",		handle_none}, 	/*59*/
{"exit",		handle_none}, 	/*60*/
{"wait4",		handle_none}, 	/*61*/
{"kill",		handle_none}, 	/*62*/
{"uname",		handle_none}, 	/*63*/
{"semget",		handle_none}, 	/*64*/
{"semop",		handle_none}, 	/*65*/
{"semctl",		handle_none}, 	/*66*/
{"shmdt",		handle_none}, 	/*67*/
{"msgget",		handle_none}, 	/*68*/
{"msgsnd",		handle_none}, 	/*69*/
{"msgrcv",		handle_none}, 	/*70*/
{"msgctl",		handle_none}, 	/*71*/
{"fcntl",		handle_none}, 	/*72*/
{"flock",		handle_none}, 	/*73*/
{"fsync",		handle_none}, 	/*74*/
{"fdatasync",		handle_none}, 	/*75*/
{"truncate",		handle_none}, 	/*76*/
{"ftruncate",		handle_none}, 	/*77*/
{"getdents",		handle_none}, 	/*78*/
{"getcwd",		handle_none}, 	/*79*/
{"chdir",		handle_none}, 	/*80*/
{"fchdir",		handle_none}, 	/*81*/
{"rename",		handle_none}, 	/*82*/
{"mkdir",		handle_none}, 	/*83*/
{"rmdir",		handle_none}, 	/*84*/
{"creat",		handle_creat}, 	/*85*/
{"link",		handle_none}, 	/*86*/
{"unlink",		handle_none}, 	/*87*/
{"symlink",		handle_none}, 	/*88*/
{"readlink",		handle_none}, 	/*89*/
{"chmod",		handle_none}, 	/*90*/
{"fchmod",		handle_none}, 	/*91*/
{"chown",		handle_none}, 	/*92*/
{"fchown",		handle_none}, 	/*93*/
{"lchown",		handle_none}, 	/*94*/
{"umask",		handle_none}, 	/*95*/
{"gettimeofday",	handle_none}, 	/*96*/
{"getrlimit",		handle_none}, 	/*97*/
{"getrusage",		handle_none}, 	/*98*/
{"sysinfo",		handle_none}, 	/*99*/
{"times",		handle_none}, 	/*100*/
{"ptrace",		handle_none}, 	/*101*/
{"getuid",		handle_none}, 	/*102*/
{"syslog",		handle_none}, 	/*103*/
{"getgid",		handle_none}, 	/*104*/
{"setuid",		handle_none}, 	/*105*/
{"setgid",		handle_none}, 	/*106*/
{"geteuid",		handle_none}, 	/*107*/
{"getegid",		handle_none}, 	/*108*/
{"setpgid",		handle_none}, 	/*109*/
{"getppid",		handle_none}, 	/*110*/
{"getpgrp",		handle_none}, 	/*111*/
{"setsid",		handle_none}, 	/*112*/
{"setreuid",		handle_none}, 	/*113*/
{"setregid",		handle_none}, 	/*114*/
{"getgroups",		handle_none}, 	/*115*/
{"setgroups",		handle_none}, 	/*116*/
{"setresuid",		handle_none}, 	/*117*/
{"getresuid",		handle_none}, 	/*118*/
{"setresgid",		handle_none}, 	/*119*/
{"getresgid",		handle_none}, 	/*120*/
{"getpgid",		handle_none}, 	/*121*/
{"setfsuid",		handle_none}, 	/*122*/
{"setfsgid",		handle_none}, 	/*123*/
{"getsid",		handle_none}, 	/*124*/
{"capget",		handle_none}, 	/*125*/
{"capset",		handle_none}, 	/*126*/
{"rt_sigpending",	handle_none}, 	/*127*/
{"rt_sigtimedwait",	handle_none}, 	/*128*/
{"rt_sigqueueinfo",	handle_none}, 	/*129*/
{"rt_sigsuspend",	handle_none}, 	/*130*/
{"sigaltstack",		handle_none}, 	/*131*/
{"utime",		handle_none}, 	/*132*/
{"mknod",		handle_none}, 	/*133*/
{"uselib",		handle_none}, 	/*134*/
{"personality",		handle_none}, 	/*135*/
{"ustat",		handle_none}, 	/*136*/
{"statfs",		handle_none}, 	/*137*/
{"fstatfs",		handle_none}, 	/*138*/
{"sysfs",		handle_none}, 	/*139*/
{"getpriority",		handle_none}, 	/*140*/
{"setpriority",		handle_none}, 	/*141*/
{"sched_setparam",	handle_none}, 	/*142*/
{"sched_getparam",	handle_none}, 	/*143*/
{"sched_setscheduler",	handle_none}, 	/*144*/
{"sched_getscheduler",	handle_none}, 	/*145*/
{"sched_get_priority_max",	handle_none}, 	/*146*/
{"sched_get_priority_min",	handle_none}, 	/*147*/
{"sched_rr_get_interval",	handle_none}, 	/*148*/
{"mlock",		handle_none}, 	/*149*/
{"munlock",		handle_none}, 	/*150*/
{"mlockall",		handle_none}, 	/*151*/
{"munlockall",		handle_none}, 	/*152*/
{"vhangup",		handle_none}, 	/*153*/
{"modify_ldt",		handle_none}, 	/*154*/
{"pivot_root",		handle_none}, 	/*155*/
{"_sysctl",		handle_none}, 	/*156*/
{"prctl",		handle_none}, 	/*157*/
{"arch_prctl",		handle_none}, 	/*158*/
{"adjtimex",		handle_none}, 	/*159*/
{"setrlimit",		handle_none}, 	/*160*/
{"chroot",		handle_none}, 	/*161*/
{"sync",		handle_none}, 	/*162*/
{"acct",		handle_none}, 	/*163*/
{"settimeofday",	handle_none}, 	/*164*/
{"mount",		handle_none}, 	/*165*/
{"umount2",		handle_none}, 	/*166*/
{"swapon",		handle_none}, 	/*167*/
{"swapoff",		handle_none}, 	/*168*/
{"reboot",		handle_none}, 	/*169*/
{"sethostname",		handle_none}, 	/*170*/
{"setdomainname",	handle_none}, 	/*171*/
{"iopl",		handle_none}, 	/*172*/
{"ioperm",		handle_none}, 	/*173*/
{"create_module",	handle_none}, 	/*174*/
{"init_module",		handle_none}, 	/*175*/
{"delete_module",	handle_none}, 	/*176*/
{"get_kernel_syms",	handle_none}, 	/*177*/
{"query_module",	handle_none}, 	/*178*/
{"quotactl",		handle_none}, 	/*179*/
{"nfsservctl",		handle_none}, 	/*180*/
{"getpmsg",		handle_none}, 	/*181*/
{"putpmsg",		handle_none}, 	/*182*/
{"afs_syscall",		handle_none}, 	/*183*/
{"tuxcall",		handle_none}, 	/*184*/
{"security",		handle_none}, 	/*185*/
{"gettid",		handle_none}, 	/*186*/
{"readahead",		handle_none}, 	/*187*/
{"setxattr",		handle_none}, 	/*188*/
{"lsetxattr",		handle_none}, 	/*189*/
{"fsetxattr",		handle_none}, 	/*190*/
{"getxattr",		handle_none}, 	/*191*/
{"lgetxattr",		handle_none}, 	/*192*/
{"fgetxattr",		handle_none}, 	/*193*/
{"listxattr",		handle_none}, 	/*194*/
{"llistxattr",		handle_none}, 	/*195*/
{"flistxattr",		handle_none}, 	/*196*/
{"removexattr",		handle_none}, 	/*197*/
{"lremovexattr",	handle_none}, 	/*198*/
{"fremovexattr",	handle_none}, 	/*199*/
{"tkill",		handle_none}, 	/*200*/
{"time",		handle_none}, 	/*201*/
{"futex",		handle_none}, 	/*202*/
{"sched_setaffinity",	handle_none}, 	/*203*/
{"sched_getaffinity",	handle_none}, 	/*204*/
{"set_thread_area",	handle_none}, 	/*205*/
{"io_setup",		handle_none}, 	/*206*/
{"io_destroy",		handle_none}, 	/*207*/
{"io_getevents",	handle_none}, 	/*208*/
{"io_submit",		handle_none}, 	/*209*/
{"io_cancel",		handle_none}, 	/*210*/
{"get_thread_area",	handle_none}, 	/*211*/
{"lookup_dcookie",	handle_none}, 	/*212*/
{"epoll_create",	handle_none}, 	/*213*/
{"epoll_ctl_old",	handle_none}, 	/*214*/
{"epoll_wait_old",	handle_none}, 	/*215*/
{"remap_file_pages",	handle_none}, 	/*216*/
{"getdents64",		handle_none}, 	/*217*/
{"set_tid_address",	handle_none}, 	/*218*/
{"restart_syscall",	handle_none}, 	/*219*/
{"semtimedop",		handle_none}, 	/*220*/
{"fadvise64",		handle_none}, 	/*221*/
{"timer_create",	handle_none}, 	/*222*/
{"timer_settime",	handle_none}, 	/*223*/
{"timer_gettime",	handle_none}, 	/*224*/
{"timer_getoverrun",	handle_none}, 	/*225*/
{"timer_delete",	handle_none}, 	/*226*/
{"clock_settime",	handle_none}, 	/*227*/
{"clock_gettime",	handle_none}, 	/*228*/
{"clock_getres",	handle_none}, 	/*229*/
{"clock_nanosleep",	handle_none}, 	/*230*/
{"exit_group",		handle_none}, 	/*231*/
{"epoll_wait",		handle_none}, 	/*232*/
{"epoll_ctl",		handle_none}, 	/*233*/
{"tgkill",		handle_none}, 	/*234*/
{"utimes",		handle_none}, 	/*235*/
{"vserver",		handle_none}, 	/*236*/
{"mbind",		handle_none}, 	/*237*/
{"set_mempolicy",	handle_none}, 	/*238*/
{"get_mempolicy",	handle_none}, 	/*239*/
{"mq_open",		handle_none}, 	/*240*/
{"mq_unlink",		handle_none}, 	/*241*/
{"mq_timedsend",	handle_none}, 	/*242*/
{"mq_timedreceive",	handle_none}, 	/*243*/
{"mq_notify",		handle_none}, 	/*244*/
{"mq_getsetattr",	handle_none}, 	/*245*/
{"kexec_load",		handle_none}, 	/*246*/
{"waitid",		handle_none}, 	/*247*/
{"add_key",		handle_none}, 	/*248*/
{"request_key",		handle_none}, 	/*249*/
{"keyctl",		handle_none}, 	/*250*/
{"ioprio_set",		handle_none}, 	/*251*/
{"ioprio_get",		handle_none}, 	/*252*/
{"inotify_init",	handle_none}, 	/*253*/
{"inotify_add_watch",	handle_none}, 	/*254*/
{"inotify_rm_watch",	handle_none}, 	/*255*/
{"migrate_pages",	handle_none}, 	/*256*/
{"openat",		handle_openat}, 	/*257*/
{"mkdirat",		handle_none}, 	/*258*/
{"mknodat",		handle_none}, 	/*259*/
{"fchownat",		handle_none}, 	/*260*/
{"futimesat",		handle_none}, 	/*261*/
{"newfstatat",		handle_none}, 	/*262*/
{"unlinkat",		handle_none}, 	/*263*/
{"renameat",		handle_none}, 	/*264*/
{"linkat",		handle_none}, 	/*265*/
{"symlinkat",		handle_none}, 	/*266*/
{"readlinkat",		handle_none}, 	/*267*/
{"fchmodat",		handle_none}, 	/*268*/
{"faccessat",		handle_none}, 	/*269*/
{"pselect6",		handle_none}, 	/*270*/
{"ppoll",		handle_none}, 	/*271*/
{"unshare",		handle_none}, 	/*272*/
{"set_robust_list",	handle_none}, 	/*273*/
{"get_robust_list",	handle_none}, 	/*274*/
{"splice",		handle_none}, 	/*275*/
{"tee",			handle_none}, 	/*276*/
{"sync_file_range",	handle_none}, 	/*277*/
{"vmsplice",		handle_none}, 	/*278*/
{"move_pages",		handle_none}, 	/*279*/
{"utimensat",		handle_none}, 	/*280*/
{"epoll_pwait",		handle_none}, 	/*281*/
{"signalfd",		handle_none}, 	/*282*/
{"timerfd_create",	handle_none}, 	/*283*/
{"eventfd",		handle_none}, 	/*284*/
{"fallocate",		handle_none}, 	/*285*/
{"timerfd_settime",	handle_none}, 	/*286*/
{"timerfd_gettime",	handle_none}, 	/*287*/
{"paccept",		handle_none}, 	/*288*/
{"signalfd4",		handle_none}, 	/*289*/
{"eventfd2",		handle_none}, 	/*290*/
{"epoll_create1",	handle_none}, 	/*291*/
{"dup3",		handle_none}, 	/*292*/
{"pipe2",		handle_none}, 	/*293*/
{"inotify_init1",	handle_none}, 	/*294*/

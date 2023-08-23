#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>

#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>

#ifdef DEBUG
#include "log.h"
#include <assert.h>
#else
#define assert(...) (void)0
#define log_info(...) (void)0
#define log_debug(...) (void)0
#define log_warn(...) (void)0
#define log_error(...) (void)0
#endif

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define die(msg)                                                               \
  {                                                                            \
    perror(msg);                                                               \
    exit(1);                                                                   \
  }

#define LOCK(mutex_ptr, callback)                                              \
  pthread_mutex_lock(mutex_ptr);                                               \
  callback;                                                                    \
  pthread_mutex_unlock(mutex_ptr);

void *get_in_addr(struct sockaddr *sa);
char *addr_to_string(struct sockaddr *addr, socklen_t len);
float calc_duration(struct timespec *start_time, struct timespec *end_time);


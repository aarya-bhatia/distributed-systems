/** A library of common header files, utility and networking functions **/
#pragma once

// standard
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// networking
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>

// external
#include "log.h"

// cpp headers
#include <list>
#include <string>
#include <vector>

#define BLOCK_SIZE 1024

#define MAX(a, b) ((a) > (b) ? (a) : (b))
#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Exit program with an error message */
#define die(msg)                                                               \
  {                                                                            \
    perror(msg);                                                               \
    exit(1);                                                                   \
  }

/* wrap a block of code with a mutex lock */
#define SAFE(mutex, callback)                                                  \
  pthread_mutex_lock(&mutex);                                                  \
  callback;                                                                    \
  pthread_mutex_unlock(&mutex);

// Utility functions

char *trimwhitespace(
    char *str); /* erase leading and trailing whitesapce from string and return
                   pointer to first non whitespace character. */
char *make_string(
    char *format,
    ...); /* allocates a string from format string and args with exact size */
char *rstrstr(char *string,
              char *pattern); /* reverse strstr: returns pointer to last
                                 occurrence of pattern in string */

/* calculates time difference in milliseconds */
float calc_duration(struct timespec *start_time, struct timespec *end_time);

/* splits a string by whitespace and allocates a string array with the tokens */
char **split_string(const char *str);

/* Returns a static datetime string in the form of "yyyy/mm/dd-hh:mm:ss.ms" */
const char *get_datetime();

std::vector<std::string>
readlines(const char *filename);  /* Returns a vector of lines in given file */
size_t word_len(const char *str); /* calculates the distance to the next
                                     whitespace in the string */

// Networking functions

/* Starts a TCP server on given port and returns the listening socket */
int start_server(int port, int backlog);

int connect_to_host(
    const char *hostname,
    const char *port); /* Attempts to establish TCP connection with given host
                          and return socket fd */
void *get_in_addr(
    struct sockaddr *sa); /* returns the in_addr of ivp4 and ipv6 addresses */
int get_port(struct sockaddr *sa);
char *addr_to_string(struct sockaddr *addr,
                     socklen_t len); /* get ip address from sockaddr */

ssize_t read_all(int fd, char *buf, size_t len);
ssize_t write_all(int fd, char *buf, size_t len);

#include "common.h"

/**
 * Use this utility function to allocate a string with given format and args.
 * The purpose of this function is to check the size of the resultant string
 * after all substitutions and allocate only those many bytes.
 */
char *make_string(char *format, ...) {
  va_list args;

  // Find the length of the output string

  va_start(args, format);
  int n = vsnprintf(NULL, 0, format, args);
  va_end(args);

  // Create the output string

  char *s = (char *)calloc(1, n + 1);

  va_start(args, format);
  vsprintf(s, format, args);
  va_end(args);

  return s;
}

/**
 * Note: This function returns a pointer to a substring of the original string.
 * If the given string was allocated dynamically, the caller must not overwrite
 * that pointer with the returned value, since the original pointer must be
 * deallocated using the same allocator with which it was allocated.  The return
 * value must NOT be deallocated using free() etc.
 *
 */
char *trimwhitespace(char *str) {
  char *end;

  // Trim leading space
  while (isspace((unsigned char)*str))
    str++;

  if (*str == 0) // All spaces?
    return str;

  // Trim trailing space
  end = str + strlen(str) - 1;
  while (end > str && isspace((unsigned char)*end))
    end--;

  // Write new null terminator character
  end[1] = '\0';

  return str;
}

/**
 * Return a pointer to the last occurrence of substring "pattern" in
 * given string "string". Returns NULL if pattern not found.
 */
char *rstrstr(char *string, char *pattern) {
  char *next = strstr(string, pattern);
  char *prev = next;

  while (next) {
    next = strstr(prev + strlen(pattern), pattern);

    if (next) {
      prev = next;
    }
  }

  return prev;
}

std::vector<char *> readlines(const char *filename) {
  std::vector<char *> lines;
  FILE *file = fopen(filename, "r");

  if (!file) {
    log_error("Failed to open file %s", filename);
    return lines;
  }

  char *line = NULL;
  size_t len = 0;
  ssize_t nread;

  // Second line contains channel topic
  while ((nread = getline(&line, &len, file)) > 0) {
    assert(line);

    if (strlen(line) == 0) {
      continue;
    }

    size_t n = strlen(line);

    if (line[n - 1] == '\n') {
      line[n - 1] = 0;
    }

    lines.push_back(line);
  }

  free(line);
  fclose(file);

  return lines;
}

/**
 * Find the length of word in string at the given position.
 */
size_t word_len(const char *str) {
  const char *tok = strstr(str, " ");

  if (!tok) {
    return strlen(str);
  } else {
    return tok - str;
  }
}

/**
 * Attempts to establish tcp connection with a server on given address
 * Returns a socket on success and -1 on failure.
 */
int connect_to_host(const char *hostname, const char *port) {
  struct addrinfo hints, *info = NULL;

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  int ret;

  if ((ret = getaddrinfo(hostname, port, &hints, &info)) == -1) {
    log_error("getaddrinfo() failed: %s", gai_strerror(ret));
    return -1;
  }

  int fd;

  struct addrinfo *itr = NULL;

  for (itr = info; itr != NULL; itr = itr->ai_next) {
    fd = socket(itr->ai_family, itr->ai_socktype, itr->ai_protocol);

    if (fd == -1) {
      continue;
    }

    if (connect(fd, itr->ai_addr, itr->ai_addrlen) != -1) {
      break; /* Success */
    }

    close(fd);
  }

  freeaddrinfo(info);

  if (!itr) {
    log_error("Failed to connect to server %s:%s", hostname, port);
    return -1;
  }

  log_info("connected to server %s on port %s", hostname, port);

  return fd;
}

/**
 * Returns the in_addr part of a sockaddr struct of either ipv4 or ipv6 type.
 */
void *get_in_addr(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    return &(((struct sockaddr_in *)sa)->sin_addr);
  }

  return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

int get_port(struct sockaddr *sa) {
  if (sa->sa_family == AF_INET) {
    struct sockaddr_in *sin = (struct sockaddr_in *)sa;
    return ntohs(sin->sin_port);
  } else {
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)sa;
    return ntohs(sin6->sin6_port);
  }
}

/**
 * Returns a pointer to a static string containing the IP address
 * of the given sockaddr.
 */
char *addr_to_string(struct sockaddr *addr, socklen_t len) {
  static char s[100];
  inet_ntop(addr->sa_family, get_in_addr(addr), s, len);
  return s;
}

/**
 * Used to read at most len bytes from fd into buffer.
 */
ssize_t read_all(int fd, char *buf, size_t len) {
  size_t bytes_read = 0;

  while (bytes_read < len) {
    errno = 0;
    ssize_t ret = read(fd, buf + bytes_read, len - bytes_read);

    if (ret >= 0) {
      bytes_read += ret;
      buf[bytes_read] = 0;

      if ((size_t)ret < len - bytes_read) {
        break;
      }
    } else if (ret == -1 && errno == EINTR) {
      continue;
    } else {
      return -1;
    }
  }

  return bytes_read;
}

/**
 * Used to write at most len bytes of buf to fd.
 */
ssize_t write_all(int fd, char *buf, size_t len) {
  size_t bytes_written = 0;

  while (bytes_written < len) {
    errno = 0;
    ssize_t ret = write(fd, buf + bytes_written, len - bytes_written);

    if (ret == 0) {
      break;
    } else if (ret > 0) {
      bytes_written += ret;
      continue;
    } else if (ret == -1 && errno == EINTR) {
      continue;
    } else {
      return -1;
    }
  }

  return bytes_written;
}

/* calculates time duration in milliseconds */
float calc_duration(struct timespec *start_time, struct timespec *end_time) {
  clock_gettime(CLOCK_MONOTONIC, end_time);

  return (end_time->tv_nsec - start_time->tv_nsec) * 1E-6 +
         (end_time->tv_sec - start_time->tv_sec) * 1E3;
}

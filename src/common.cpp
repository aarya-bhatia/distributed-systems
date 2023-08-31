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

std::vector<std::string> readlines(const char *filename) {
  std::vector<std::string> lines;
  FILE *file = fopen(filename, "r");

  if (!file) {
    log_error("Failed to open file %s", filename);
    return lines;
  }

  char *line = NULL;
  size_t len = 0;
  ssize_t nread;

  while ((nread = getline(&line, &len, file)) > 0) {
    assert(line);

    size_t n = strlen(line);

    if (n && line[n - 1] == '\n') {
      line[n - 1] = 0;
    }

    // empty line
    if (*line == 0) {
      continue;
    }

    lines.push_back(std::string(line));
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
 * Start a TCP server on the given port.
 * Returns a socket on success or -1 on failure.
 */
int start_server(int port, int backlog) {
  int listen_sock = socket(PF_INET, SOCK_STREAM, 0);
  if (listen_sock == -1) {
    die("socket");
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = INADDR_ANY;

  int yes = 1;
  if (setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) !=
      0) {
    die("setsockopt");
  }

  if (bind(listen_sock, (struct sockaddr *)&addr, sizeof(struct sockaddr_in)) !=
      0) {
    die("bind");
  }

  if (listen(listen_sock, backlog) != 0) {
    die("listen");
  }

  log_info("Server is running on port %d", port);

  return listen_sock;
}

/**
 * Attempts to establish TCP connection with a host
 * Returns a socket on success or -1 on failure.
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
const char *addr_to_string(struct sockaddr *addr, socklen_t len) {
  static char s[100];
  inet_ntop(addr->sa_family, get_in_addr(addr), s, len);
  return s;
}

/**
 * To read len bytes from fd into buf.
 */
ssize_t read_all(int fd, char *buf, size_t len) {
  size_t nread = 0;
  while (nread < len) {
    size_t to_read = MIN(BLOCK_SIZE, len - nread);
    ssize_t ret = read(fd, buf + nread, to_read);
    if (ret == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (ret == 0) {
      break;
    } else {
      nread += ret;
    }
  }

  return nread;
}

/**
 * To write len bytes from buf to fd.
 */
ssize_t write_all(int fd, char *buf, size_t len) {
  size_t nsent = 0;
  while (nsent < len) {
    size_t to_write = MIN(BLOCK_SIZE, len - nsent);
    ssize_t ret = write(fd, buf + nsent, to_write);
    if (ret == -1) {
      if (errno == EINTR) {
        continue;
      } else {
        return -1;
      }
    } else if (ret == 0) {
      break;
    } else {
      nsent += ret;
    }
  }

  return nsent;
}

/* calculates time duration in milliseconds */
float calc_duration(struct timespec *start_time, struct timespec *end_time) {
  return (end_time->tv_nsec - start_time->tv_nsec) * 1E-6 +
         (end_time->tv_sec - start_time->tv_sec) * 1E3;
}

/**
 * Splits a string by whitespace.
 * Returns a null-terminated array of string tokens.
 */
char **split_string(const char *str) {
  assert(str);

  char **tokens = (char **)calloc(1, sizeof *tokens);
  size_t cap = 1;
  size_t len = 0;

  if (*str == 0) {
    // no tokens
    return tokens;
  }

  do {
    const char *first = str;
    const char *second = strstr(first, " ");

    // resize array, if required
    if (len + 1 >= cap) {
      cap *= 2;
      tokens = (char **)realloc(tokens, cap * sizeof *tokens);
    }

    if (second) {
      tokens[len++] = strndup(first, second - first);
      str = second + 1;
    } else {
      tokens[len++] = strdup(first);
      break;
    }
  } while (str != NULL);

  if (len + 1 >= cap) {
    cap *= 2;
    tokens = (char **)realloc(tokens, cap * sizeof *tokens);
  }

  tokens[len++] = NULL;

  return tokens;
}

/**
 * Get UTC datetime string
 */
const char *get_datetime() {
  static char buf[64];
  time_t t = time(NULL);
  struct tm tm = *gmtime(&t); // Get UTC time
  strftime(buf, sizeof buf, "%Y/%m/%e-%H:%M:%S", &tm);
  return buf;
}

/**
* Convert bytes to human readble string
*/
const char *humanSize(uint64_t bytes)
{
	const char *suffix[] = {"B", "KB", "MB", "GB", "TB"};
	char length = sizeof(suffix) / sizeof(suffix[0]);

	int i = 0;
	double dblBytes = bytes;

	if (bytes > 1024) {
		for (i = 0; (bytes / 1024) > 0 && i<length-1; i++, bytes /= 1024)
			dblBytes = bytes / 1024.0;
	}

	static char output[200];
	sprintf(output, "%.02lf %s", dblBytes, suffix[i]);
	return output;
}

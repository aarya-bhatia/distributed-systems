#include "common.h"
#include "queue.h"
#include <cstdlib>
#include <list>
#include <pthread.h>
#include <unordered_map>
#include <vector>

#define SERVER_PORT "6000"

Queue *msg_queue = NULL;
char command[1024] = {0};

struct Host {
  const char *hostname;
  const char *port;
  Host(const char *h, const char *p) : hostname(h), port(p) {}
};

struct Task {
  Host *host;
  pthread_t tid;
  int status;
};

static Host hosts[] = {Host("127.0.0.1", "6000"), Host("127.0.0.1", "6001"),
                       Host("127.0.0.1", "6002")};

void *worker(void *args) {
  Task *task = (Task *)args;
  log_info("Worker thread %ld started", task->tid);

  int fd = connect_to_host(task->host->hostname, task->host->port);

  if (fd == -1) {
    task->status = EXIT_FAILURE;
    return args;
  }

  if (write_all(fd, command, strlen(command)) == -1) {
    log_error("Thread %ld: write_all", task->tid);
    task->status = EXIT_FAILURE;
    close(fd);
    return args;
  }

  char buffer[1024];
  size_t off = 0;

  while (1) {
    ssize_t n_read = read(fd, buffer, sizeof buffer);
    if (n_read == -1 && errno == EINTR) {
      continue;
    } else if (n_read <= 0) {
      break;
    } else if (n_read > 0) {
      off += n_read;
      buffer[n_read] = 0;

      char *s = strstr(buffer, "\n");
      bool f = false;

      if (s) {
        *s = 0;
        printf("Host %s:%s: %s\n", task->host->hostname, task->host->port,
                 buffer);
        f = 1;
        memmove(buffer, s + 1, strlen(s + 1) + 1);
        off = strlen(buffer);
      }

      if ((size_t)n_read < sizeof buffer) {
        if (!f) {
          printf("Host %s:%s: %s\n", task->host->hostname, task->host->port,
                   buffer);
        }
        break;
      }
    } else {
      log_error("Thread %ld: read", task->tid);
      task->status = EXIT_FAILURE;
      close(fd);
      return args;
    }
  }

  close(fd);

  task->status = EXIT_SUCCESS;
  return args;
}

int main(int argc, const char *argv[]) {
  if (argc == 1) {
    fprintf(stderr, "Usage: %s command [...options]\n", *argv);
    return 0;
  }

  size_t num_hosts = sizeof hosts / sizeof hosts[0];
  // size_t num_hosts = 1;

  Task *tasks = (Task *)calloc(num_hosts, sizeof *tasks);

  for (size_t i = 0; i < num_hosts; i++) {
    tasks[i].host = &hosts[i];
    tasks[i].status = -1;
    pthread_create(&tasks[i].tid, NULL, worker, &tasks[i]);
  }

  size_t off = 0;
  for (int i = 1; i < argc; i++) {
    off += sprintf(command + off, "%s", argv[i]);
    if (i + 1 < argc) {
      off += sprintf(command + off, " ");
    }
  }

  log_debug("Command: %s", command);

  size_t count = 0;

  for (size_t i = 0; i < num_hosts; i++) {
    pthread_join(tasks[i].tid, NULL);
    if (tasks[i].status == EXIT_SUCCESS) {
      count++;
    }
  }

  log_info("Got reply from %ld out of %ld hosts.", count, num_hosts);

  free(tasks);

  return 0;
}

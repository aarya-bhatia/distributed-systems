#include "common.h"
#include "queue.h"
#include <cstdlib>
#include <fcntl.h>
#include <list>
#include <pthread.h>
#include <unordered_map>
#include <vector>

#define SERVER_PORT "6000"

struct Host {
  int id;
  char *hostname;
  char *port;
  Host(int _id, const char *_hostname, const char *_port)
      : id(_id), hostname(strdup(_hostname)), port(strdup(_port)) {}

  ~Host() {
    free(hostname);
    free(port);
  }
};

struct Task {
  Host *host;
  pthread_t tid;
  int status;
  struct timespec start_time;
  struct timespec end_time;
  unsigned long total_bytes_recv;
};

struct Message {
  enum MessageType { TYPE_DATA, TYPE_FINISHED } type;
  void *data;

  Message(MessageType type, void *data) : type(type), data(data) {}
};

Queue<Message *> *msg_queue = NULL;
char command[1024] = {0};
static std::vector<Host *> hosts;

void load_hosts(const char *filename) {
  std::vector<std::string> lines = readlines(filename);
  for (size_t i = 0; i < lines.size(); i++) {
    if (lines[i].empty() || lines[i][0] == '!') {
      continue;
    }

    char *tmp = strdup(lines[i].c_str());

    const char *id = strtok(tmp, " ");
    const char *host = strtok(NULL, " ");
    const char *port = strtok(NULL, " ");

    hosts.push_back(new Host(atoi(id), host, port));

    free(tmp);
  }
}

void *worker(void *args) {
  Task *task = (Task *)args;
  assert(task);
  assert(strlen(command));

  char hostname[40];
  sprintf(hostname, "%s:%s", task->host->hostname, task->host->port);

  int fd = connect_to_host(task->host->hostname, task->host->port);

  if (fd == -1) {
    msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
    task->status = EXIT_FAILURE;
    return args;
  }

  clock_gettime(CLOCK_REALTIME, &task->start_time);

  if (write_all(fd, command, strlen(command)) <= 0) {
    perror("write");
    close(fd);
    msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
    task->status = EXIT_FAILURE;
    return args;
  }

  shutdown(fd, SHUT_WR);

  char buffer[MAX_BUFFER_LEN];
  ssize_t nread;
  size_t off = 0;
  size_t total = 0;
  while ((nread = read_all(fd, buffer + off, sizeof buffer - off - 1)) > 0) {
    total += nread;
    off += nread;

    buffer[off] = 0;

    char *last = rstrstr(buffer, (char *)"\n");
    if (!last) {
      continue;
    }

    *last = 0;

    char *ptr = strtok(buffer, "\n");
    while (ptr) {
      size_t len = strlen(ptr);
      if (len) {
        char *msg = NULL;
        asprintf(&msg, "%s (%zu bytes): %s", hostname, len, ptr);
        msg_queue->enqueue(new Message(Message::TYPE_DATA, msg));
      }
      ptr = strtok(NULL, "\n");
    }

    if (last[1] != 0 && last < buffer + off) {
      size_t remaining_bytes = strlen(last + 1);
      memmove(last + 1, buffer, remaining_bytes);
      off = remaining_bytes;
    } else {
      off = 0;
    }

    buffer[off] = 0;
  }

  shutdown(fd, SHUT_RD);
  close(fd);

  if (nread < 0) {
    perror("read");
    task->status = EXIT_FAILURE;
    msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
    close(fd);
    return args;
  }

  task->total_bytes_recv = total;
  task->status = EXIT_SUCCESS;
  msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));

  clock_gettime(CLOCK_REALTIME, &task->end_time);

  return args;
}

int main(int argc, const char *argv[]) {
  if (argc == 1) {
    fprintf(stderr, "Usage: %s command [...options]\n", *argv);
    return 0;
  }

  load_hosts("hosts");

  if (hosts.empty()) {
    log_warn("%s", "There are no hosts.");
    return 0;
  }

  msg_queue = new Queue<Message *>();

  size_t off = 0;
  for (int i = 1; i < argc; i++) {
    off += sprintf(command + off, "%s", argv[i]);
    if (i + 1 < argc) {
      off += sprintf(command + off, " ");
    } else {
      off += sprintf(command + off, "\n");
    }
  }

  Task *tasks = (Task *)calloc(hosts.size(), sizeof *tasks);

  for (size_t i = 0; i < hosts.size(); i++) {
    tasks[i].host = hosts[i];
    tasks[i].status = -1;
    pthread_create(&tasks[i].tid, NULL, worker, &tasks[i]);
  }

  size_t finished = 0;

  while (finished < hosts.size()) {
    Message *m = (Message *)msg_queue->dequeue();

    if (m->type == Message::TYPE_DATA && m->data != NULL) {
      puts((char *)m->data);
      free((char *)m->data);
    } else if (m->type == Message::TYPE_FINISHED) {
      Task *task = (Task *)m->data;
      log_debug("Task completed for host %s:%s", task->host->hostname,
                task->host->port);
      log_debug("Tasks remaining: %zd", hosts.size() - finished);
      finished++;
    }

    delete m;
  }

  size_t count = 0;
  uint64_t total_bytes_recv = 0;
  double latency = 0;

  FILE *output_file = fopen("latency_output", "a");

  if (!output_file) {
    die("Failed to open output file");
  }

  for (size_t i = 0; i < hosts.size(); i++) {
    pthread_join(tasks[i].tid, NULL);
    if (tasks[i].status == EXIT_SUCCESS) {
      count++;
      double time_elapsed_millisecond =
          calc_duration(&tasks[i].start_time, &tasks[i].end_time);
      printf("Time elapsed for task %ld (%s:%s): %f\n", i,
             tasks[i].host->hostname, tasks[i].host->port,
             time_elapsed_millisecond);
      latency += time_elapsed_millisecond;
      fprintf(output_file, "%f ", time_elapsed_millisecond);

      printf("Total bytes read from %s:%s: %zu\n", tasks[i].host->hostname,
             tasks[i].host->port, tasks[i].total_bytes_recv);
      total_bytes_recv += tasks[i].total_bytes_recv;
    }
  }

  fprintf(output_file, "\n");
  fclose(output_file);

  double average_latency = count ? latency / count : 0;

  printf("=== RESULT === Got reply from %ld out of %ld hosts with an average "
         "latency of %0.4f "
         "milliseconds.\n",
         count, hosts.size(), average_latency);

  printf("Total data received from %ld hosts: %s\n", count,
         humanSize(total_bytes_recv));

  free(tasks);

  for (size_t i = 0; i < hosts.size(); i++) {
    delete hosts[i];
  }

  delete msg_queue;

  return 0;
}

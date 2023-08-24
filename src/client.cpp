#include "common.h"
#include "queue.h"
#include <cstdlib>
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

  void print_info() { log_debug("Host %d: %s:%s", id, hostname, port); }
};

struct Task {
  Host *host;
  pthread_t tid;
  int status;
};

struct Message {
  enum MessageType { TYPE_DATA, TYPE_FINISHED } type;
  void *data;

  Message(MessageType type, void *data) : type(type), data(data) {}
};

Queue *msg_queue = NULL;
char command[1024] = {0};
static std::vector<Host *> hosts;

void load_hosts(const char *filename) {
  std::vector<std::string> lines = readlines(filename);
  for (size_t i = 0; i < lines.size(); i++) {
    if (lines[i].empty()) {
      continue;
    }

    char *tmp = strdup(lines[i].c_str());

    const char *id = strtok(tmp, " ");
    const char *host = strtok(NULL, " ");
    const char *port = strtok(NULL, " ");

    hosts.push_back(new Host(atoi(id), host, port));
    hosts.back()->print_info();

    free(tmp);
  }
}

void *worker(void *args) {
  Task *task = (Task *)args;
  log_info("Worker thread %ld started", task->tid);

  int fd = connect_to_host(task->host->hostname, task->host->port);

  if (fd == -1) {
    msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
    task->status = EXIT_FAILURE;
    return args;
  }

  if (write_all(fd, command, strlen(command)) == -1) {
    log_error("Thread %ld: write_all", task->tid);
    task->status = EXIT_FAILURE;
    close(fd);
    msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
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

      char *msg = NULL;

      if (s) {
        *s = 0;
        asprintf(&msg, "Host %d: %s:%s: %s", task->host->id,
                 task->host->hostname, task->host->port, buffer);
        msg_queue->enqueue(new Message(Message::TYPE_DATA, msg));

        f = 1;
        memmove(buffer, s + 1, strlen(s + 1) + 1);
        off = strlen(buffer);
      }

      if ((size_t)n_read < sizeof buffer) {
        if (!f) {
          asprintf(&msg, "Host %d: %s:%s: %s", task->host->id,
                   task->host->hostname, task->host->port, buffer);
          msg_queue->enqueue(new Message(Message::TYPE_DATA, msg));
        }
        break;
      }
    } else {
      log_error("Thread %ld: read", task->tid);
      task->status = EXIT_FAILURE;
      close(fd);
      msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
      return args;
    }
  }

  close(fd);

  task->status = EXIT_SUCCESS;
  msg_queue->enqueue(new Message(Message::TYPE_FINISHED, task));
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

  msg_queue = new Queue();

  Task *tasks = (Task *)calloc(hosts.size(), sizeof *tasks);

  for (size_t i = 0; i < hosts.size(); i++) {
    tasks[i].host = hosts[i];
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

  size_t finished = 0;

  while (finished < hosts.size()) {
    Message *m = (Message *)msg_queue->dequeue();

    if (m->type == Message::TYPE_DATA && m->data != NULL) {
      puts((char *)m->data);
      free((char *)m->data);
    } else if (m->type == Message::TYPE_FINISHED) {
      Task *task = (Task *)m->data;
      log_info("Task %ld finished: %zd remaining", task->tid,
               hosts.size() - finished);
      finished++;
    }

    delete m;

    // sleep(1);

  }

  size_t count = 0;

  for (size_t i = 0; i < hosts.size(); i++) {
    pthread_join(tasks[i].tid, NULL);
    if (tasks[i].status == EXIT_SUCCESS) {
      count++;
    }
  }

  log_info("Got reply from %ld out of %ld hosts.", count, hosts.size());

  free(tasks);

  for (size_t i = 0; i < hosts.size(); i++) {
    delete hosts[i];
  }

  delete msg_queue;

  return 0;
}

#include "common.h"
#include "queue.h"
#include <list>
#include <pthread.h>
#include <unordered_map>
#include <vector>

#define SERVER_PORT "6000"

Queue msg_queue;
char message[4096];
int fd;
char *line = NULL;
size_t cap = 0;

int main() {
  fd = connect_to_host("127.0.0.1", SERVER_PORT);

  if (fd == -1) {
    die("Failed to connect to server");
  }

  printf("Connected to server on port %s\n", SERVER_PORT);

  printf("$ ");

  if (getline(&line, &cap, stdin) > 0) {
    size_t n = strlen(line);
    line[n - 1] = 0; // Erase newline character

    if (write_all(fd, line, strlen(line)) == -1) {
      die("write_all");
    }

    if (read_all(fd, message, sizeof message) == -1) {
      die("read_all");
    }

    printf("Message recieved from server: %s\n", message);
  }

  close(fd);

  return 0;
}

#include "common.h"
#include <list>
#include <pthread.h>
#include <unordered_map>
#include <vector>

int main() {
  printf("Hello\n");

  int fd = connect_to_host("127.0.0.1", "6000");

  if (fd == -1) {
    die("Failed to connect to server");
  }

  printf("Connected to server...\n");

  char message[4096];

  sprintf(message, "grep -P \"Hello\"");

  if (write_all(fd, message, strlen(message)) == -1) {
    die("write_all");
  }

  if (read_all(fd, message, sizeof message) == -1) {
    die("read_all");
  }

  printf("Message recieved from server: %s\n", message);

  close(fd);

  return 0;
}

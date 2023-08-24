#include "common.h"
#include <list>
#include <pthread.h>
#include <unordered_map>
#include <vector>

#define PORT 6000

int main() {
  int listen_sock = socket(PF_INET, SOCK_STREAM, 0);
  if (listen_sock == -1) {
    die("socket");
  }

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(PORT);
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

  if (listen(listen_sock, 16) != 0) {
    die("listen");
  }

  log_info("Server is running on port %d", PORT);

  while (1) {
    int client_sock = accept(listen_sock, NULL, NULL);

    if (client_sock == -1) {
      continue;
    }

    int pid = fork();

    if (pid < 0) {
      die("fork");
    }

    if (pid == 0) {
      close(listen_sock);

      char message[4096];
      ssize_t nread = read(client_sock, message, sizeof message - 1);

      if (nread == -1) {
        die("read_all");
      }

      message[nread] = 0;

      printf("Message received on socket %d: %s\n", client_sock, message);

      sprintf(message, "OK");
      write_all(client_sock, message, strlen(message));

      shutdown(client_sock, SHUT_RDWR);
      close(client_sock);

      exit(0);
    } else {
      close(client_sock);
    }
  }

  close(listen_sock);
}

#include "common.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#define READ 0
#define WRITE 1
#define SUCCESS 0
#define FAILURE -1

FILE *log_file = NULL;

int send_command_output(char **command, int client_sock) {
  int pipefd[2];

  if (pipe(pipefd) < 0) {
    perror("pipe");
    return FAILURE;
  }

  pid_t pid = fork();

  if (pid < 0) {
    perror("fork");
    close(pipefd[0]);
    close(pipefd[1]);
    exit(1);
  }

  if (pid == 0) {
    close(pipefd[READ]);
    dup2(pipefd[WRITE], 1);
    close(pipefd[WRITE]);
    execvp(command[0], (char **)command);
    die("execvp");
  } else {
    close(pipefd[WRITE]);
    char buf[1024];
    ssize_t bytes_read = 0;

    while ((bytes_read = read(pipefd[READ], buf, sizeof buf)) > 0) {
      write_all(client_sock, buf, bytes_read);
      write_all(STDOUT_FILENO, buf, bytes_read);
    }

    close(pipefd[READ]);
    wait(NULL);
  }

  puts("=============================");

  return SUCCESS;
}

int main(int argc, const char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s id port\n", *argv);
    return 1;
  }

  int server_id = atoi(argv[1]);
  int port = atoi(argv[2]);

  signal(SIGPIPE, SIG_IGN);

  char filename[256];
  sprintf(filename, "/var/log/cs425/machine.%d.log", server_id);
  log_file = fopen(filename, "a");

  if (!log_file) {
    die("fopen");
  }

  int listen_sock = start_server(port);

  struct sockaddr_storage client_arr;
  socklen_t client_len = sizeof client_arr;

  while (1) {
    int client_sock =
        accept(listen_sock, (struct sockaddr *)&client_arr, &client_len);

    if (client_sock == -1) {
      continue;
    }

    const char *client_addr_str =
        addr_to_string((struct sockaddr *)&client_arr, client_len);

    char message[256];
    sprintf(message, "Connected to client: %s", client_addr_str);
    logger(log_file, message);

    int pid = fork();

    if (pid < 0) {
      die("fork");
    }

    if (pid == 0) {
      close(listen_sock);

      char message[4096];
      ssize_t nread = read(client_sock, message, sizeof message - 1);

      if (nread == -1) {
        die("read");
      }

      message[nread] = 0;

      printf("Message received on socket %d: %s\n", client_sock, message);

      char **command = split_string(message);
      send_command_output(command, client_sock);

      for (char **s = command; *s != NULL; s++) {
        free(*s);
      }

      free(command);

      shutdown(client_sock, SHUT_RDWR);
      close(client_sock);

      exit(0);
    } else {
      close(client_sock);
    }
  }

  close(listen_sock);

  fclose(log_file);
}

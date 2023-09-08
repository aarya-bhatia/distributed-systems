#include "common.h"
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#define READ 0
#define WRITE 1
#define SUCCESS 0
#define FAILURE -1

#define MAX_REQUEST_LEN 4096

/**
 * Data to pass to thread per connection
 */
struct Connection {
  pthread_t tid;
  int client_sock;
  struct sockaddr_storage client_addr;
  socklen_t client_len;
};

static int server_id;
static int port;

void logger(const char *message) {
  assert(message);
  static pthread_mutex_t logger_mutex = PTHREAD_MUTEX_INITIALIZER;
  static bool logger_init = false;
  static char filename[256];

  pthread_mutex_lock(&logger_mutex);

  if (!logger_init) {
    sprintf(filename, "logs/machine.%d.log", server_id);

    if (system("mkdir -p logs") < 0) {
      die("system");
    }

    logger_init = true;
  }

  FILE *log_file = fopen(filename, "a");

  if (!log_file) {
    die("fopen");
  }

  fprintf(log_file, "%s: %s\n", get_datetime(), message);
  fclose(log_file);

  pthread_mutex_unlock(&logger_mutex);
}

/**
 * Runs a shell command in a child process and sends its output to the client
 * using a pipe.
 *
 * @param command Array of strings containing shell command and args
 * @param client_sock TCP socket to write the output of the command to
 *
 * Returns a status of SUCCESS or FAILURE.
 */
int send_command_output(char **command, int client_sock) {
  assert(command);
  assert(command[0] != NULL);
  assert(client_sock >= 0);

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
    log_error("cannot exec command %s", command[0]);
    exit(1);
  } else {
    close(pipefd[WRITE]);
    char buf[MAX_BUFFER_LEN];
    ssize_t bytes_read = 0;

    while ((bytes_read = read_all(pipefd[READ], buf, sizeof buf)) > 0) {
      if (write_all(client_sock, buf, bytes_read) == -1) {
        break;
      }
    }

    write_all(client_sock, (char *)"\n", 1);

    close(pipefd[READ]);
    wait(NULL);
  }

  return SUCCESS;
}

/**
 * Thread function for each tcp connection
 */
void *worker(void *args) {
  Connection *conn = (Connection *)args;
  assert(conn);

  char client_addr_str[40];

  sprintf(
      client_addr_str, "%s:%d",
      addr_to_string((struct sockaddr *)&conn->client_addr, conn->client_len),
      get_port((struct sockaddr *)&conn->client_addr));

  log_info("Connected to client: %s", client_addr_str);

  char message[MAX_REQUEST_LEN + 1];
  ssize_t nread = read_all(conn->client_sock, message, MAX_REQUEST_LEN);

  if (nread <= 0) {
    close(conn->client_sock);
    delete conn;
    perror("read");
    return NULL;
  }

  message[nread] = 0;

  shutdown(conn->client_sock, SHUT_RD);

  if (message[nread - 1] == '\n') {
    message[nread - 1] = 0;
  }

  if (strlen(message) == 0) {
    shutdown(conn->client_sock, SHUT_WR);
    close(conn->client_sock);
    delete conn;
    return NULL;
  }

  log_info("Request from client (%zu bytes) %s: %s", strlen(message),
           client_addr_str, message);

  char **command = split_string(message);

  if (send_command_output(command, conn->client_sock) == FAILURE) {
    perror("send_command_output");
  }

  shutdown(conn->client_sock, SHUT_WR);
  close(conn->client_sock);

  for (char **s = command; *s != NULL; s++) {
    free(*s);
  }
  free(command);

  delete conn;
  return NULL;
}

/**
 * Server accepts an ID and port on start up.
 */
int main(int argc, const char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s id port\n", *argv);
    return 1;
  }

  server_id = atoi(argv[1]);
  port = atoi(argv[2]);

  signal(SIGPIPE, SIG_IGN);

  int listen_sock = start_server(port, 1024);

  struct sockaddr_storage client_addr;
  socklen_t client_len = sizeof client_addr;
  pthread_t tid;

  while (1) {
    int client_sock =
        accept(listen_sock, (struct sockaddr *)&client_addr, &client_len);

    if (client_sock == -1) {
      continue;
    }

    Connection *conn = new Connection;
    conn->tid = tid;
    conn->client_sock = client_sock;
    conn->client_addr = client_addr;
    conn->client_len = client_len;

    pthread_create(&tid, NULL, worker, (void *)conn);
    pthread_detach(tid);
  }

  close(listen_sock);
  exit(0);
}

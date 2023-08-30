#include "common.h"
#include "queue.h"
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

/**
 * Data to pass to thread per connection
 */
struct Connection {
  int tid;
  int client_sock;
  struct sockaddr_storage client_addr;
  socklen_t client_len;
};

static Queue<char *> *msg_queue = NULL; // Message queue that receives strings
static int server_id;
static int port;

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
      if (write(client_sock, buf, bytes_read) == -1) {
        break;
      }
    }

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

  char client_addr_str[40];

  sprintf(
      client_addr_str, "%s:%d",
      addr_to_string((struct sockaddr *)&conn->client_addr, conn->client_len),
      get_port((struct sockaddr *)&conn->client_addr));

  log_info("Connected to client: %s", client_addr_str);

  msg_queue->enqueue(
      make_string((char *)"Connected to client: %s", client_addr_str));

  char message[4096];
  ssize_t nread = read(conn->client_sock, message, sizeof message - 1);

  if (nread == -1) {
    close(conn->client_sock);
    delete conn;
    perror("read");
    return NULL;
  }

  if (nread == 0) {
    log_warn("No bytes received from client %s", client_addr_str);
    close(conn->client_sock);
    delete conn;
    return NULL;
  }

  message[nread] = 0;

  shutdown(conn->client_sock, SHUT_RD);

  log_debug("Request from socket %d: %s", conn->client_sock, message);

  char *log_message = make_string((char *)"Request from client %s: %s",
                                  client_addr_str, message);

  msg_queue->enqueue(log_message);

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
 * Listens for messages (strings) on the message queue and writes them to the
 * log file with the appropriate format. It will free the memory for the message
 * string.
 */
void *file_logger_start(void *args) {
  Queue<char *> *msg_queue = (Queue<char *> *)args;

  char filename[256];
  system("mkdir -p logs");
  sprintf(filename, "logs/machine.%d.log", server_id);
  log_info("Started file logger thread %ld: %s", pthread_self(), filename);

  FILE *log_file = fopen(filename, "a");

  if (!log_file) {
    die("fopen");
  }

  while (1) {
    char *message = (char *)msg_queue->dequeue();
    if (!message) {
      break;
    }
    logger(log_file, message);
    fflush(log_file);
    // log_debug("Message received on thread %ld: %s", pthread_self(), message);
    free(message);
  }

  fclose(log_file);

  return args;
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

  msg_queue = new Queue<char *>;

  signal(SIGPIPE, SIG_IGN);

  int listen_sock = start_server(port, 1024);

  pthread_t file_logger_tid;
  pthread_create(&file_logger_tid, NULL, file_logger_start, msg_queue);
  sleep(1);

  struct sockaddr_storage client_addr;
  socklen_t client_len = sizeof client_addr;
  pthread_t tid;

  while (1) {
    int client_sock =
        accept(listen_sock, (struct sockaddr *)&client_addr, &client_len);

    if (client_sock == -1) {
      continue;
    }

    if (fcntl(client_sock, F_SETFD, fcntl(client_sock, F_GETFD) | O_NONBLOCK) <
        0) {
      perror("fcntl");
      close(client_sock);
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

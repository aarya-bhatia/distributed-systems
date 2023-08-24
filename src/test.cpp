#include "common.h"
#include <cstdlib>
#include <fcntl.h>
#include <sys/wait.h>

#define READ 0
#define WRITE 1

int main() {
  int pipefd[2];

  if (pipe(pipefd) < 0) {
    die("pipe");
  }

  pid_t pid = fork();

  if (pid < 0) {
    die("fork");
  }

  if (pid == 0) {
    close(pipefd[READ]);
    dup2(pipefd[WRITE], 1);
    close(pipefd[WRITE]);
    // execlp("echo", "echo", "hello world", NULL);
    execlp("grep", "grep", "", "NOTES.md", NULL);
    die("execlp");
  } else {
    close(pipefd[WRITE]);
    char buf[1024];
    ssize_t bytes_read = 0;

    while ((bytes_read = read(pipefd[READ], buf, sizeof buf)) > 0) {
      write(STDOUT_FILENO, buf, bytes_read);
    }

    close(pipefd[READ]);
    wait(NULL);
  }
}

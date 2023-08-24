#pragma once

#include <list>
#include <pthread.h>

struct Queue {
  std::list<void *> l;
  pthread_mutex_t m;
  pthread_cond_t cv;

  Queue();
  ~Queue();

  void enqueue(void *data);
  void *dequeue();
};

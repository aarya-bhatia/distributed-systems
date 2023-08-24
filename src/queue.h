#pragma once

#include <pthread.h>
#include <list>

template<typename T>
struct Queue
{
  std::list<T *> l;
	pthread_mutex_t m;
	pthread_cond_t cv;

  Queue();
  ~Queue();

  void enqueue(T *data);
  T *dequeue();
};


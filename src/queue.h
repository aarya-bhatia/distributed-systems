#pragma once

#include <list>
#include <pthread.h>

/**
 * A blocking queue data structure safe to use with multiple threads
 */
template <typename T> struct Queue {
  std::list<T> l;
  pthread_mutex_t m;
  pthread_cond_t cv;

  Queue() {
    pthread_mutex_init(&m, NULL);
    pthread_cond_init(&cv, NULL);
  }

  ~Queue() {
    pthread_mutex_destroy(&m);
    pthread_cond_destroy(&cv);
  }

  void enqueue(const T &data) {
    pthread_mutex_lock(&m);
    l.push_back(data);
    pthread_cond_signal(&cv);
    pthread_mutex_unlock(&m);
  }

  T dequeue() {
    pthread_mutex_lock(&m);
    while (l.empty()) {
      pthread_cond_wait(&cv, &m);
    }
    T data = l.front();
    l.pop_front();
    pthread_cond_signal(&cv);
    pthread_mutex_unlock(&m);
    return data;
  }
};

#include "queue.h"

template <typename T> Queue<T>::Queue() {
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&cv, NULL);
}

template <typename T> Queue<T>::~Queue() {
  for (T *t : l) {
    delete t;
  }

  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&cv);
}

template <typename T> void Queue<T>::enqueue(T *data) {
  pthread_mutex_lock(&m);
  l.push_back(data);
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&m);
}

template <typename T> T *Queue<T>::dequeue() {
  pthread_mutex_lock(&m);
  while (l.empty()) {
    pthread_cond_wait(&cv, &m);
  }
  T *data = l.front();
  l.pop_front();
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&m);
  return data;
}

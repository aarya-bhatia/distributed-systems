#include "queue.h"

Queue::Queue() {
  pthread_mutex_init(&m, NULL);
  pthread_cond_init(&cv, NULL);
}

Queue::~Queue() {
  pthread_mutex_destroy(&m);
  pthread_cond_destroy(&cv);
}

void Queue::enqueue(void *data) {
  pthread_mutex_lock(&m);
  l.push_back(data);
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&m);
}

void *Queue::dequeue() {
  pthread_mutex_lock(&m);
  while (l.empty()) {
    pthread_cond_wait(&cv, &m);
  }
  void *data = l.front();
  l.pop_front();
  pthread_cond_signal(&cv);
  pthread_mutex_unlock(&m);
  return data;
}

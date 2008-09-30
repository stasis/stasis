#ifndef __QUEUE_H
#define __QUEUE_H

struct queue;

struct queue *queue_new (int);
void queue_free (struct queue *);
unsigned long queue_deq (struct queue *);
int queue_enq (struct queue *, unsigned long);

#endif /* __QUEUE_H */


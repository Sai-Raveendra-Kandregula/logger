#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <czmq.h>

#define PRODUCERS_COUNT 3
#define ADDITIONAL_THREADS_PER_CONSUMER 2
#define CONSUMERS_COUNT 1

void* log_broker(void* arg)
{
    printf("Initializing Log Broker...\n");

    /// In-socket for the logger
    zsock_t *broker_in = zsock_new_router ("ipc:///tmp/cfw/logger_in");

    printf("Log Broker Initialized.\n");
    printf("Listening...\n");
    while (1) {
        char *flagger = zstr_recv (broker_in); ///< Flag to identify Producer (or) Signal to the Log Broker from Main Thread
        if(flagger && strcmp(flagger, "END") == 0) {
            break;
        }
        if(flagger && !(strcmp(flagger, "") == 0)) {
            printf("%s\n", flagger);
        }
        zstr_free(&flagger);
    }

    zsock_destroy (&broker_in);
    pthread_exit(NULL);
}

void* producer(void* arg){
    zsock_t *pusher = zsock_new_dealer ("ipc:///tmp/cfw/logger_in");
    int pro_num = (int)arg;

    printf("Producer #: %d\n", pro_num);
    for (int i = 1; i <= 5; i++)
    {
        sleep(1); ///< Simulating some work being done
        zstr_sendf (pusher, "Log number - %d", pro_num);
    }
    zsock_destroy (&pusher);
    pthread_exit(NULL);
}

void main(){
    pthread_t log_broker_thread;
    pthread_t producers[PRODUCERS_COUNT];

    /// Used to send shutdown signal to the logging in socket. Function can be extended, like added consumers, etc.
    zsock_t *log_signaler = zsock_new_dealer("ipc:///tmp/cfw/logger_in");

    /// Creating Producer threads
    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_create(&(producers[i]), NULL, &producer, (void*)i);
    }

    /// Creating Log Broker thread
    pthread_create(&log_broker_thread, NULL, &log_broker, NULL);

    /// Join all Producers before Termination.

    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_join( producers[i], NULL );
    }

    zstr_send(log_signaler, "END");
    pthread_join( log_broker_thread, NULL );
    zsock_destroy(&log_signaler);
    exit(0);
}

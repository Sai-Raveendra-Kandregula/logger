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
#define ADDITIONAL_THREADS_PER_CONSUMER 2 ///< Number of additional workers per Consumer.
#define CONSUMERS_COUNT 1 ///< Total Number of Consumers.
#define MAX_ENDPOINT_STR_LENGTH 50 ///< Length limit for Worker and Consumer Endpoints.

/// Signalling messages used by the sockets
#define INIT_SIGNAL "INIT" ///< Message used to initialise connection b/w Router and Dealer Sockets.
#define READY_SIGNAL "READY" ///< Message used as Ready Signal.
#define END_SIGNAL "END" ///< Message used as Terminate Signal.

/**
* This is a worker function for the Log Broker to offload consumer transmissions
* @param[in] arg An char array containing worker_id and consumer_endpoint, cast to a void pointer.
*/
void* consumer(void* arg){
    zsock_t *consumer = zsock_new_pull("ipc:///tmp/cfw/cons_in_1");

    printf("Consumer is UP.\n");
    while (1) {
        char *msg = zstr_recv ( consumer );
        if(msg && strcmp(msg, END_SIGNAL) == 0) {
            printf("Recieved %s Signal for Consumer.\n", END_SIGNAL);
            break;
        }
        if(msg && !(strcmp(msg, "") == 0)) {
            printf("%s\n", msg);
        }
        zstr_free(&msg);
    }

    printf("Consumer Terminated.\n");
    zsock_destroy(&consumer);
    pthread_exit(NULL);
}

/**
* Checks index of the worker and returns if the index is Zero, i.e. that is the first created worker for the consumer
*/
int isFirstWorkerForConsumer(char *worker_transport){
   int cons, worker;
   sscanf(worker_transport, "ipc:///tmp/cfw/c_%dw_%d", &cons, &worker);
   return (worker == 0);
}

void* log_worker(void* arg){
    char *worker_transport = ((char**)arg)[0];
    char *consumer_endpoint = ((char**)arg)[1];

    // printf("Inhouse Transport - %s - Initializing...\n", worker_transport);
    printf("Consumer Endpoint - %s\n", consumer_endpoint);

    /// Socket to send messages to Consumer
    zsock_t *consumer = zsock_new_push(consumer_endpoint);
    // sleep(0.1);

    /// Socket to listen for messages from Broker
    zsock_t *worker_listener = zsock_new_pull(worker_transport);

    while (1) {
        char *msg = zstr_recv ( worker_listener );
        if(msg && strcmp(msg, END_SIGNAL) == 0) {
            /// First Worker of each Consumer (based on creation index) sends END Signal to Consumer.
            if(isFirstWorkerForConsumer(worker_transport)) { printf("Killing Consumer.\n"); zstr_send( consumer, END_SIGNAL ); }
            printf("%s - Terminating...\n", worker_transport);
            break;
        }
        if(msg && !(strcmp(msg, "") == 0)) {
            // printf("%s\n", msg);
	    zstr_send( consumer, msg );
        }
        zstr_free(&msg);
    }

    zsock_destroy(&worker_listener);
    zsock_destroy(&consumer);

    pthread_exit(NULL);
}

void* log_broker(void* arg)
{
    printf("Initializing Log Broker...\n");

    /// In-socket for the logger
    zsock_t *broker_in = zsock_new_router ("ipc:///tmp/cfw/logger_in");

    printf("Log Broker Initialized.\nConsumer Workers Initializing...\n");

    pthread_t consumer_thread;
    /// Create Consumer thread
    pthread_create( &consumer_thread, NULL, &consumer, NULL );

    char consumers[CONSUMERS_COUNT][MAX_ENDPOINT_STR_LENGTH] = { "ipc:///tmp/cfw/cons_in_1" };

    int worker_sockets_count = CONSUMERS_COUNT * ADDITIONAL_THREADS_PER_CONSUMER;
    zsock_t **worker_sockets = malloc( worker_sockets_count * sizeof(zsock_t*) );

    for (int cons = 0; cons < CONSUMERS_COUNT; cons++){
        for(int worker = 0; worker < ADDITIONAL_THREADS_PER_CONSUMER; worker++){
            char **params = malloc( sizeof(char*) * 2 );
            params[0] = malloc( sizeof(char) * MAX_ENDPOINT_STR_LENGTH );
            sprintf(params[0], "ipc:///tmp/cfw/c_%dw_%d", cons, worker); ///< Worker Listener Endpoint to use for further communication
            params[1] = malloc( sizeof(char) * MAX_ENDPOINT_STR_LENGTH );
            strcpy(params[1], consumers[cons]); ///< Consumer Destination Endpoint

            /// Thread for each Worker
	    pthread_t worker_thread;
            pthread_create(&worker_thread, NULL, &log_worker, (void*)params);

	    /// Socket to communicate with Workers
            worker_sockets[(cons * ADDITIONAL_THREADS_PER_CONSUMER) + worker] = zsock_new_push(params[0]);
        }
    }

    printf("Consumer Workers Initialized.\n");
    int isShutdownRequested = 0;

    int *isReady = (int*)arg;
    *isReady = 1;

    /// Start Listening to Messages
    printf("Listening...\n");
    while (!isShutdownRequested) {
            for(int worker = 0; worker < ADDITIONAL_THREADS_PER_CONSUMER; worker++){
	        for(int consumer = 0; consumer < CONSUMERS_COUNT; consumer++){
 		    char *msg = zstr_recv (broker_in); ///< Flag to identify Producer (or) Signal from Main Thread to the Log Broker
        	    if(msg && strcmp(msg, END_SIGNAL) == 0) {
            	        isShutdownRequested = 1;
			break;
        	    }
                    if( msg && !(strcmp(msg, "") == 0) ) {
	      	        // printf("From Worker\n");
		        zstr_send( worker_sockets[ (consumer * ADDITIONAL_THREADS_PER_CONSUMER) + worker ] , msg);
            	    }
		    zstr_free( &msg );
		}
                if(isShutdownRequested) break;
	    }

        if(isShutdownRequested) break;
    }

    /// Send END signal to Workers
    for(int worker = 0; worker < worker_sockets_count; worker++){
        zstr_send( worker_sockets[ worker ] , END_SIGNAL);
        zsock_destroy( &worker_sockets[worker] );
    }

    /// Join the Consumer Thread before Termination
    pthread_join( consumer_thread, NULL );

    zsock_destroy (&broker_in);
    pthread_exit(NULL);
}

void* producer(void* arg){
    zsock_t *pusher = zsock_new_dealer ("ipc:///tmp/cfw/logger_in");
    int pro_num = (int)arg;

    printf("Producer %d is now UP.\n", pro_num);
    for (int i = 1; i <= 5; i++)
    {
        sleep(1); ///< Simulating some work being done
        zstr_sendf (pusher, "Log number - %d", pro_num);
    }

    zsock_destroy (&pusher);

    printf("Producer %d is now Terminated.\n", pro_num);
    pthread_exit(NULL);
}

void main(){
    pthread_t log_broker_thread;
    pthread_t producers[PRODUCERS_COUNT];

    /// Used to send shutdown signal to the logging in socket. Function can be extended, like added consumers, etc.
    zsock_t *log_signaler = zsock_new_dealer("ipc:///tmp/cfw/logger_in");

    int isLogBrokerSocketInitialized = 0;
    /// Creating Log Broker thread
    pthread_create(&log_broker_thread, NULL, &log_broker, (void*)(&isLogBrokerSocketInitialized));

    printf("Waiting for Log Broker to start...\n");
    while(isLogBrokerSocketInitialized == 0){}

    /// Creating Producer threads
    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_create(&(producers[i]), NULL, &producer, (void*)i);
    }

    /// Join all Producers before Termination.

    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_join( producers[i], NULL );
    }

    zstr_send(log_signaler, "END");
    pthread_join( log_broker_thread, NULL );
    zsock_destroy(&log_signaler);
    exit(0);
}

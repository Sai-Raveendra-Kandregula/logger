#define _GNU_SOURCE
#include <sched.h>
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <czmq.h>

#define PRODUCERS_COUNT 3 ///< Total Number of Producers
#define PER_PRODUCER_MSG_COUNT 100 ///< Number of messages each Producer should Send
#define MSG_MAX_LENGTH 50 ///< Max length of message sent by Producer

int u_PRODUCERS_COUNT = 0, u_PER_PRODUCER_MSG_COUNT = 0, bind_to_cores = 0;

#define ADDITIONAL_THREADS_PER_CONSUMER 2 ///< Number of additional workers per Consumer.
#define CONSUMERS_COUNT 1 ///< Total Number of Consumers.


#define MAX_ENDPOINT_STR_LENGTH 50 ///< Length limit for Worker and Consumer Endpoints.

/// Signalling messages used by the sockets
#define INIT_SIGNAL "INIT" ///< Message used to initialise connection b/w Router and Dealer Sockets.
#define READY_SIGNAL "READY" ///< Message used as Ready Signal.
#define END_SIGNAL "END" ///< Message used as Terminate Signal.


/// Function that Binds Calling Thread to specified CPU Core
int stick_this_thread_to_core(int core_id) {
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;

   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();    
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
}

/**
* This is a worker function for the Log Broker to offload consumer transmissions
* @param[in] arg An char array containing worker_id and consumer_endpoint, cast to a void pointer.
*/
void* consumer(void* arg){
    zsock_t *consumer = zsock_new_pull("ipc:///tmp/cfw/cons_in_1");

    // printf("Consumer is UP.\n");
    while (1) {
        char *msg = zstr_recv ( consumer );

        /// Signal Handling
        if(msg && strcmp(msg, END_SIGNAL) == 0) {
            // printf("Recieved %s Signal for Consumer.\n", END_SIGNAL);
            break;
        }
        /// End of Signal Handling

        if(msg && !(strcmp(msg, "") == 0)) {
            printf("%s\n", msg);
        }
        zstr_free(&msg);
    }

    // printf("Consumer Terminated.\n");
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
    char *worker_transport = ((char**)arg)[0]; ///< Transport via which Log Broker Communicates with Worker
    char *consumer_endpoint = ((char**)arg)[1]; ///< Transport via which Worker communicates with Consumer

    // printf("Inhouse Transport - %s - Initializing...\n", worker_transport);
    // printf("Consumer Endpoint - %s\n", consumer_endpoint);

    /// Socket to send messages to Consumer
    zsock_t *consumer = zsock_new_push(consumer_endpoint);
    // sleep(0.1);

    /// Socket to listen for messages from Broker
    zsock_t *worker_listener = zsock_new_pull(worker_transport);

    while (1) {
        char *msg = zstr_recv ( worker_listener );

        /// Signal Handling
        if(msg && strcmp(msg, END_SIGNAL) == 0) {
            /// First Worker of each Consumer (based on creation index) sends END Signal to Consumer.
            if(isFirstWorkerForConsumer(worker_transport)) { zstr_send( consumer, END_SIGNAL ); }
            // printf("%s - Terminating...\n", worker_transport);
            break;
        }
        // End of Signal Handling

        if(msg && !(strcmp(msg, "") == 0)) {
	    zstr_send( consumer, msg ); ///< Forward Message to Consumer
        }
        zstr_free(&msg);
    }

    zsock_destroy(&worker_listener);
    zsock_destroy(&consumer);

    pthread_exit(NULL);
}

/**
* Broker Thread which acts as first Point of Contact for Producers
* @param[in] arg An int pointer to indicate the readiness of the Broker Thread
*/
void* log_broker(void* arg)
{
    if(bind_to_cores) stick_this_thread_to_core(sysconf(_SC_NPROCESSORS_ONLN) - 1); // Use last core for log broker
    // printf("Initializing Log Broker...\n");

    /// In-socket for the logger
    zsock_t *broker_in = zsock_new_router ("ipc:///tmp/cfw/logger_in");

    // printf("Log Broker Initialized.\nConsumer Workers Initializing...\n");
    char inflow_timestamps[u_PRODUCERS_COUNT * u_PER_PRODUCER_MSG_COUNT][(MSG_MAX_LENGTH * 2) + 1];
    int message_count = 0;

    pthread_t consumer_thread;
    /// Create Consumer thread
    pthread_create( &consumer_thread, NULL, &consumer, NULL );

    /// Consumer Endpoints Array
    char consumers[CONSUMERS_COUNT][MAX_ENDPOINT_STR_LENGTH] = { "ipc:///tmp/cfw/cons_in_1" };

    /// Consumer Worker Sockets Array
    int worker_sockets_count = CONSUMERS_COUNT * ADDITIONAL_THREADS_PER_CONSUMER;
    zsock_t **worker_sockets = malloc( worker_sockets_count * sizeof(zsock_t*) );

    /// Creating Sockets for Each Consumer Worker
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

    // printf("Consumer Workers Initialized.\n");
    int isShutdownRequested = 0;

    int *isReady = (int*)arg;
    *isReady = 1;

    /// Start Listening to Messages
    // printf("Listening...\n");
    while (!isShutdownRequested) {
            for(int worker = 0; worker < ADDITIONAL_THREADS_PER_CONSUMER; worker++){
	        for(int consumer = 0; consumer < CONSUMERS_COUNT; consumer++){
 		    char *msg = zstr_recv (broker_in); ///< Flag to identify Producer (or) Signal from Main Thread to the Log Broker

                    /// Signal Handling
        	    if(msg && strcmp(msg, END_SIGNAL) == 0) {
            	        isShutdownRequested = 1;
			break;
        	    }
                    // Signal Handling Ends

                    if( msg && !(strcmp(msg, "") == 0) ) {
		        zstr_send( worker_sockets[ (consumer * ADDITIONAL_THREADS_PER_CONSUMER) + worker ] , msg);
                        struct timespec ts;
    			timespec_get(&ts, TIME_UTC);
			char buff[100];
			strftime(buff, sizeof buff, "%D %T", gmtime(&ts.tv_sec));
                        sprintf(inflow_timestamps[message_count], "%s,%s.%09ld", msg, buff, ts.tv_nsec);
                        message_count++;
            	    }
		    zstr_free( &msg );
		}
                if(isShutdownRequested) break;
	    }

        if(isShutdownRequested) break;
    }


    char filename[50];
    if(bind_to_cores) sprintf(filename, "results_raw/%dP_%dM_bound.csv", u_PRODUCERS_COUNT, u_PER_PRODUCER_MSG_COUNT);
    else sprintf(filename, "results_raw/%dP_%dM.csv", u_PRODUCERS_COUNT, u_PER_PRODUCER_MSG_COUNT);
    FILE *fp = fopen(filename, "w");
    fclose(fp);
    fp = fopen(filename, "a");
	if(fp)//will be null if failed to open
	{
	    for(int i = 0; i < message_count; i++){
		fprintf(fp, "%s\r\n", inflow_timestamps[i]);
		}
            fclose(fp);
	}
        else{
	    for(int i = 0; i < message_count; i++){
                printf("%s\n", inflow_timestamps[i]);
                }
	}
    /// Send END signal to Workers
    for(int worker = 0; worker < worker_sockets_count; worker++){
        zstr_send( worker_sockets[ worker ] , END_SIGNAL);
        zsock_destroy( &worker_sockets[worker] );
    }

    /// Join the Consumer Thread before Termination
    pthread_join( consumer_thread, NULL );

    zsock_destroy (&broker_in);

    // printf("Log Broker Terminated.\n");
    pthread_exit(NULL);
}

void* producer(void* arg){
    int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
    int pro_num = (int)arg;
    if(bind_to_cores) stick_this_thread_to_core(pro_num % num_cores);

    zsock_t *pusher = zsock_new_dealer ("ipc:///tmp/cfw/logger_in");
    // printf("Producer %d is now UP.\n", pro_num);
    for (int i = 1; i <= PER_PRODUCER_MSG_COUNT; i++)
    {
        // sleep(1); ///< Simulating some work being done
	struct timespec ts;
        timespec_get(&ts, TIME_UTC);
	char buff[100];
	strftime(buff, sizeof buff, "%D %T", gmtime(&ts.tv_sec));
        zstr_sendf (pusher, "%s.%09ld", buff, ts.tv_nsec); ///< Send Message to Log Broker
    }

    /// Await any pending Transmissions
    sleep(5);
    zsock_destroy (&pusher);
    // printf("Producer %d is now Terminated.\n", pro_num);
    pthread_exit(NULL);
}

int main(int argc, char **argv){
    for (int i = 1; i < argc; i++){
        if(strcmp(argv[i], "--np") == 0){
            int val;
	    i++;
            if( sscanf(argv[i], "%d", &val) == 1 && val > 0 ){
		u_PRODUCERS_COUNT = val;
                printf("Using %d for Number of Producers.\n", u_PRODUCERS_COUNT);
            }
            else{
                printf("%s is a invalid value for --np\n", argv[i]);
            }
        }
        else if(strcmp(argv[i], "--nm") == 0){ ///< Messages per Producer
            int val;
            i++;
            if( sscanf(argv[i], "%d", &val) == 1 && val > 0 ){
                u_PER_PRODUCER_MSG_COUNT = val;
                printf("Using %d for Messages/Producers.\n", u_PER_PRODUCER_MSG_COUNT);
            }
            else{
                printf("%s is a invalid value for --nm\n", argv[i]);
            }
        }
        // if(bind_to_cores)
        else if(strcmp(argv[i], "--bind") == 0){ ///< Messages per Producer
            bind_to_cores = 1;
            printf("Binding Producers and Logging thread to Cores.\n");
        }
    }

    if(u_PRODUCERS_COUNT == 0){
          u_PRODUCERS_COUNT = PRODUCERS_COUNT;
          printf("Using Default value of %d for Number of Producers.\n", PRODUCERS_COUNT);
    }

    if(u_PER_PRODUCER_MSG_COUNT == 0){
         u_PER_PRODUCER_MSG_COUNT = PER_PRODUCER_MSG_COUNT;
         printf("Using Default value of %d for Messages/Producer.\n", PER_PRODUCER_MSG_COUNT);
    }

    pthread_t log_broker_thread;
    pthread_t producers[u_PRODUCERS_COUNT];

    /// Used to send shutdown signal to the logging in socket. Function can be extended, like added consumers, etc.
    zsock_t *log_signaler = zsock_new_dealer("ipc:///tmp/cfw/logger_in");

    int isLogBrokerSocketInitialized = 0;
    /// Creating Log Broker thread
    pthread_create(&log_broker_thread, NULL, &log_broker, (void*)(&isLogBrokerSocketInitialized));

    // printf("Waiting for Log Broker to start...\n");
    while(isLogBrokerSocketInitialized == 0){}

    /// Creating Producer threads
    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_create(&(producers[i]), NULL, &producer, (void*)i);
    }

    /// Join all Producers to Main Thread before Termination.
    for (int i = 0; i < PRODUCERS_COUNT; i++){
        pthread_join( producers[i], NULL );
    }

    /// Send END signal to LogBroker
    zstr_send(log_signaler, "END");
    pthread_join( log_broker_thread, NULL );
    zsock_destroy(&log_signaler);
    return 0; // exit(0);
}

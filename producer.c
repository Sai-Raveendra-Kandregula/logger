#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <zmq.h>

void* log_message(void* arg)
{
    // detach the current thread
    // from the calling thread
    // pthread_detach(pthread_self());

    // printf("Inside Thread.\n");

    void *context = zmq_ctx_new ();
    void *puller = zmq_socket (context, ZMQ_SUB);
    int rc = zmq_connect(puller, "ipc:///tmp/cfw/firewall");
    assert(rc == 0);
    rc = zmq_setsockopt(puller, ZMQ_SUBSCRIBE, "", 0);
    assert(rc == 0);

    while (1) {
        char *msg = (char*)malloc(100*sizeof(char));
        // int recv =
        zmq_recv (puller, msg, 100, 0);
        printf("%s", msg);
        sleep(0.001);
    }

    zmq_close(puller);
    zmq_ctx_destroy (context);
    pthread_exit(NULL);
}

int main(){
    printf("Log Producer - 1\n");

    void *context = zmq_ctx_new ();
    void *pusher = zmq_socket (context, ZMQ_PUB);

    int rc = zmq_bind(pusher, "ipc:///tmp/cfw/firewall");
    assert(rc == 0);

    pthread_t logger;
    // Creating a new thread
    pthread_create(&logger, NULL, &log_message, NULL);
    sleep(1);

    for (int i = 1; i < 100; i++)
    {
        char* buffer = (char*)malloc(100*sizeof(char));
        printf("Log # - %d.\n", i);
        sprintf(buffer, "Log number - %d\n", i);
        int send = zmq_send (pusher, (char*)buffer, 100, 0);
        assert(send == 100);
    }

    zmq_ctx_destroy (context);
    return 0;
}

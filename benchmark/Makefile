CC=gcc

all: benchmark

benchmark:
	${CC} -o logger_benchmarker logger_benchmarking.c -lczmq -lpthread

clean:
	rm logger_benchmarker

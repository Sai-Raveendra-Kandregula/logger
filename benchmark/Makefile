CC=gcc

all: benchmark

benchmark:
	${CC} -o logger_benchmarker logger_benchmarking.c -lczmq -lpthread

boundedb:
	${CC} -o logger_benchmarker_bound logger_benchmarking_bound.c -lczmq -lpthread

cli:
	${CC} -o log_benchmarker_cli logger_benchmarking_cli.c -lczmq -lpthread

clean:
	rm logger_benchmarker

cleanb:
	rm logger_benchmarker_bound

cleancli:
	rm log_benchmarker_cli

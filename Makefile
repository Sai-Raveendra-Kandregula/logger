CC=gcc

all: clean logger.o
	${CC} -o logger logger.o -lczmq -lpthread

logger.o:
	${CC} -c logger.c -o logger.o

clean:
	rm *.o logger

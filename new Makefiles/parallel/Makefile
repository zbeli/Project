CC = gcc
CFLAGS = -g -I.
DEPS = result.h utils.h parse.h query.h query_selection.h str.h thread_pool.h job_scheduler.h

result.o utils.o query.o parse.o query_selection.o str.o main.o: CFLAGS+=-O3

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o utils.o parse.o query.o thread_pool.o job_scheduler.o query_selection.o
	$(CC)  -o join main.o str.o result.o utils.o parse.o query.o thread_pool.o job_scheduler.o query_selection.o -pthread -lm

clean :
	rm -f *.o join

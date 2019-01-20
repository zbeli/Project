CC = gcc
CFLAGS = -g -I.
DEPS = str.h result.h utils.h parse.h query.h query_selection.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o utils.o parse.o query.o query_selection.o
	$(CC) -o join main.o str.o result.o utils.o parse.o query.o query_selection.o -lm

clean :
rm -f *.o join

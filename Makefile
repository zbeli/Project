CC = gcc
CFLAGS = -I.
DEPS = str.h result.h utils.h parse.h query.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o utils.o parse.o query.o
	$(CC) -o join main.o str.o result.o utils.o parse.o query.o

clean :
	rm -f *.o join

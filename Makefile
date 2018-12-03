CC = gcc
CFLAGS = -I.
DEPS = str.h result.h utils.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o utils.o
	$(CC) -o join main.o str.o result.o utils.o

clean :
	rm -f *.o join

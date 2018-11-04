CC = gcc
CFLAGS = -I.
DEPS = str.h result.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o
	$(CC) -o join main.o str.o result.o

clean :
	rm -f *.o join

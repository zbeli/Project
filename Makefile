CC = gcc
CFLAGS = -I.
DEPS = str.h result.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
main: main.o str.o result.o
	$(CC) -o main main.o str.o result.o

clean :
	rm -f *.o main

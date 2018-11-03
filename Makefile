CC = gcc
CFLAGS = -I.
DEPS = str.h


%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
main: main.o str.o
	$(CC) -o main main.o str.o

clean :
	rm -f *.o main

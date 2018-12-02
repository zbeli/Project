CC = gcc
CFLAGS = -I.
DEPS =  kati.h str.h result.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)
join: main.o str.o result.o kati.o
	$(CC) -o join main.o str.o result.o kati.o

clean :
	rm -f *.o join

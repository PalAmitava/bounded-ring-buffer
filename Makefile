CC = gcc

CFLAGS = -std=c11 -Wall -Wextra -Wpedantic -Wshadow -Wformat=2 -Wnull-dereference -Wconversion -g -pthread

SRC = producer_consumer.c buffer.c
TARGET = producer_consumer

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(SRC) -o $(TARGET)

asan:
	$(CC) $(CFLAGS) -fsanitize=address $(SRC) -o pc_asan

ubsan:
	$(CC) $(CFLAGS) -fsanitize=undefined $(SRC) -o pc_ubsan

tsan:
	$(CC) $(CFLAGS) -fsanitize=thread $(SRC) -o pc_tsan

clean:
	rm -f $(TARGET) pc_asan pc_ubsan pc_tsan *.o

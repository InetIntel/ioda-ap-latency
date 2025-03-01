CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LIBS = -lrdkafka -lipmeta -lJudy -ltimeseries -lwandio -lgsl

SRCS = aggregator.c
OBJS = $(SRCS:.c=.o)

TARGET = ioda-ap-aggregator

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

install: $(TARGET)
	cp $(TARGET) /usr/local/bin/

.PHONY: all clean install

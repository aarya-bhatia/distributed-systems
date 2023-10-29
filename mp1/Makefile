CC=g++
INCLUDES=-Iserver
CFLAGS=-DDEBUG -D_GNU_SOURCE -std=c++14 -g \
	   -Wall -Wextra -Werror -Wno-pointer-arith -pedantic -O0 \
	  -Wno-gnu-zero-variadic-macro-arguments \
	  -Wno-deprecated-declarations \
	   -gdwarf-4 -MMD -MP $(INCLUDES) -c

LDFLAGS=-pthread
# LINKLIBS += -fsanitize=thread

.PHONY: all clean

all: server

server: .obj/server.o .obj/common.o
	mkdir -p bin
	$(CC) $^ -o bin/server $(LDFLAGS)

test: .obj/test.o .obj/common.o
	mkdir -p bin
	$(CC) $^ -o bin/test $(LDFLAGS)

.obj/%.o: server/%.cpp
	mkdir -p .obj
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -rf .obj/ bin/

-include .obj/*.d

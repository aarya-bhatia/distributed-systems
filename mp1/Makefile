CC=g++
INCLUDES=-Isrc
CFLAGS=-DDEBUG -D_GNU_SOURCE -std=c++14 -g \
	   -Wall -Wextra -Werror -Wno-pointer-arith -pedantic -O0 \
	  -Wno-gnu-zero-variadic-macro-arguments \
	  -Wno-deprecated-declarations \
	   -gdwarf-4 -MMD -MP $(INCLUDES) -c

LDFLAGS=-pthread
# LINKLIBS += -fsanitize=thread

.PHONY: all clean

all: bin/server bin/client bin/test

bin/server: .obj/server.o .obj/common.o
	mkdir -p bin
	$(CC) $^ -o $@ $(LDFLAGS)

bin/test: .obj/test.o .obj/common.o
	mkdir -p bin
	$(CC) $^ -o $@ $(LDFLAGS)

.obj/%.o: src/server/%.cpp
	mkdir -p .obj
	$(CC) $(CFLAGS) -o $@ $<

bin/client: $(wildcard src/client/*.go)
	go build src/client/*.go
	mv client bin/client

clean:
	rm -rf .obj/ bin/

-include .obj/*.d

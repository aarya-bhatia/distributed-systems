CC=g++
INCLUDES=-Isrc
CFLAGS=-DDEBUG -D_GNU_SOURCE -std=c++14 -g \
	   -Wall -Wextra -Werror -Wno-pointer-arith -pedantic -O0 \
	   -gdwarf-4 -MMD -MP $(INCLUDES) -c
LDFLAGS=-Llib -pthread
# LINKLIBS += -fsanitize=thread

OBJ=obj/common.o

.PHONY: all clean

all: bin/server bin/client

bin/server: obj/server.o $(OBJ)
	mkdir -p $(dir $@);
	$(CC) $^ -o $@ $(LDFLAGS)

bin/client: obj/client.o $(OBJ)
	mkdir -p $(dir $@);
	$(CC) $^ -o $@ $(LDFLAGS)

obj/%.o: src/%.cpp
	mkdir -p $(dir $@);
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -rf obj/ bin/

-include $(OBJS_DIR)/*.d

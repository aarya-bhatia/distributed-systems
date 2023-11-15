#include "common.h"

char **tokens;

int main() {
  tokens = split_string("date");
  assert(!strcmp(tokens[0], "date"));
  assert(tokens[1] == NULL);

  tokens = split_string("");
  assert(tokens[0] == NULL);

  tokens = split_string("grep -i -P 'hello' machine.1.log");
  assert(!strcmp(tokens[0], "grep"));
  assert(!strcmp(tokens[1], "-i"));
  assert(!strcmp(tokens[2], "-P"));
  assert(!strcmp(tokens[3], "'hello'"));
  assert(!strcmp(tokens[4], "machine.1.log"));
  assert(tokens[5] == NULL);

  puts(get_datetime());
}

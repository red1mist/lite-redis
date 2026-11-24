CXX := clang++
CXXFLAGS := -std=c++20 -Wall -Wextra -Iinclude
LDFLAGS :=

SRC := $(wildcard src/*.cpp)
OBJ_DIR := build
OBJ := $(SRC:src/%.cpp=$(OBJ_DIR)/%.o)

BIN := lite-redis-server
DEBUG_BIN := lite-redis-server-debug

.PHONY: all debug clean run run-debug test unit

# Default target: optimized build, used when you just run `make`.
all: CXXFLAGS += -O2
all: $(BIN)

$(BIN): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ) $(LDFLAGS)

# Debug build: no optimization (so line numbers/variables in a debugger line
# up with source), AddressSanitizer instruments every malloc/free and pointer
# access to catch leaks, use-after-free, and buffer overruns at runtime.
# Compiled straight from source (skips build/*.o) so it never accidentally
# reuses object files built with different flags by `make all`.
debug: CXXFLAGS += -g -O0 -fsanitize=address -fno-omit-frame-pointer
debug: LDFLAGS += -fsanitize=address
debug:
	$(CXX) $(CXXFLAGS) -o $(DEBUG_BIN) $(SRC) $(LDFLAGS)

# -MMD -MP: also emit build/foo.d listing the headers foo.cpp includes, so
# editing a header rebuilds every .o that depends on it (included below).
# Depending on Makefile too means changing flags (e.g. -std) rebuilds everything.
$(OBJ_DIR)/%.o: src/%.cpp Makefile | $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

-include $(OBJ:.o=.d)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

run: all
	./$(BIN)

run-debug: debug
	./$(DEBUG_BIN)

clean:
	rm -rf $(OBJ_DIR) $(BIN) $(DEBUG_BIN) $(DEBUG_BIN).dSYM $(UNIT_BIN)

# Unit test of the keyspace + expiry heap with a fake clock, under
# AddressSanitizer and UndefinedBehaviorSanitizer.
UNIT_BIN := build/store_test
unit: | $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -g -O1 -fsanitize=address,undefined -fno-sanitize-recover=undefined \
		-o $(UNIT_BIN) test/store_test.cpp src/store.cpp
	./$(UNIT_BIN)

# Unit tests, then the ASan server on a spare port with all integration tests.
test: unit debug
	./test/run_tests.sh

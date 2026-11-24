CXX := clang++
CXXFLAGS := -std=c++17 -Wall -Wextra -Iinclude
LDFLAGS :=

SRC := $(wildcard src/*.cpp)
OBJ_DIR := build
OBJ := $(SRC:src/%.cpp=$(OBJ_DIR)/%.o)

BIN := lite-redis-server
DEBUG_BIN := lite-redis-server-debug

.PHONY: all debug clean run run-debug test

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
$(OBJ_DIR)/%.o: src/%.cpp | $(OBJ_DIR)
	$(CXX) $(CXXFLAGS) -MMD -MP -c $< -o $@

-include $(OBJ:.o=.d)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

run: all
	./$(BIN)

run-debug: debug
	./$(DEBUG_BIN)

clean:
	rm -rf $(OBJ_DIR) $(BIN) $(DEBUG_BIN) $(DEBUG_BIN).dSYM

# Builds the ASan binary, runs it on a spare port, runs all tests against it.
test: debug
	./test/run_tests.sh

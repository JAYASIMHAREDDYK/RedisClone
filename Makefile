CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2 -Iinclude

ifeq ($(OS),Windows_NT)
    LDFLAGS = -lws2_32
    TARGET = redis-server.exe
    TEST_TARGET = unit_tests.exe
else
    LDFLAGS = -lpthread
    TARGET = redis-server
    TEST_TARGET = unit_tests
endif

SRCS = src/common.cpp \
       src/skiplist.cpp \
       src/dict.cpp \
       src/resp.cpp \
       src/evict.cpp \
       src/expire.cpp \
       src/aof.cpp \
       src/net.cpp \
       src/replication.cpp \
       src/server.cpp

OBJS = $(SRCS:.cpp=.o)

all: $(TARGET) $(TEST_TARGET)

$(TARGET): $(OBJS) src/main.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_TARGET): $(OBJS) tests/test_unit.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) src/main.o tests/test_unit.o $(TARGET) $(TEST_TARGET) fuzz_resp

fuzz:
	clang++ -std=c++17 -fsanitize=fuzzer,address,undefined -O1 -g -Iinclude tests/fuzz/fuzz_resp.cpp src/resp.cpp -o fuzz_resp

.PHONY: all clean fuzz

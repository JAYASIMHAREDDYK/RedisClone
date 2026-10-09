CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -Iinclude

ifeq ($(OS),Windows_NT)
    LDFLAGS = -lws2_32
    TARGET = redis-server.exe
    TEST_TARGET = unit_tests.exe
else
    LDFLAGS = -lpthread
    TARGET = redis-server
    TEST_TARGET = unit_tests
endif

SRCS = src/util.cpp \
       src/skiplist.cpp \
       src/dict.cpp \
       src/resp.cpp \
       src/eviction.cpp \
       src/expire.cpp \
       src/aof.cpp \
       src/net.cpp \
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
	rm -f $(OBJS) src/main.o tests/test_unit.o $(TARGET) $(TEST_TARGET)

.PHONY: all clean

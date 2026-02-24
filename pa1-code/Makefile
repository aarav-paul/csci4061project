CC = gcc
CFLAGS = -Wall -Wextra -g -Iinclude

SRCS = main.c table.c
OBJS = $(SRCS:.c=.o)
TARGET = mapreduce

MAP_SRC = map.c table.c
MAP_OBJ = map.o table.o
MAP_TARGET = map

REDUCE_SRC = reduce.c table.c
REDUCE_OBJ = reduce.o table.o
REDUCE_TARGET = reduce

AN = pa1
CWD = $(shell pwd | sed 's/.*\///g')

TEST_UTILS_SRCS := $(wildcard $(TEST_RESOURCES_DIR)/*.c)
TEST_UTILS_OBJS := $(TEST_UTILS_SRCS:.c=.o)
TEST_RESOURCES_DIR = ./test_cases/resources

all: $(TARGET) $(MAP_TARGET) $(REDUCE_TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS)

$(MAP_TARGET): $(MAP_OBJ)
	$(CC) $(CFLAGS) -o $@ $(MAP_OBJ)

$(REDUCE_TARGET): $(REDUCE_OBJ)
	$(CC) $(CFLAGS) -o $@ $(REDUCE_OBJ)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f ./intermediate/* ./out/* $(OBJS) $(TARGET) *.o $(MAP_TARGET) $(REDUCE_TARGET) *.txt $(TEST_RESOURCES_DIR)/table_test $(TEST_RESOURCES_DIR)/0.tbl

clean-tests:
	rm -rf ./test_results

ifdef testnum
test: test-setup all
	./testius test_cases/tests.json -v -n $(testnum)
else
test: test-setup all
	./testius test_cases/tests.json
endif

test-setup: $(TEST_RESOURCES_DIR)/table_test
	@chmod u+x testius

$(TEST_RESOURCES_DIR)/table_test: $(TEST_RESOURCES_DIR)/table_test.c table.c
	$(CC) $(CFLAGS) $^ -o $@

zip: clean clean-tests
	rm -f $(AN)-code.zip
	cd .. && zip "$(CWD)/$(AN)-code.zip" -r "$(CWD)" -x "$(CWD)/test_cases/*" "$(CWD)/testius" "$(CWD)/logs/*" "$(CWD)/images/*" "$(CWD)/WRITEUP.md" "$(CWD)/WRITEUP.pdf"
	@echo Zip created in $(AN)-code.zip
	@if [ `stat -c '%s' $(AN)-code.zip 2>/dev/null || stat -f '%z' $(AN)-code.zip` -gt 10485760 ]; then echo "WARNING: $(AN)-code.zip seems REALLY big, check there are no abnormally large test files"; du -h $(AN)-code.zip; fi
	@if [ `unzip -t $(AN)-code.zip 2>/dev/null | wc -l` -gt 256 ]; then echo "WARNING: $(AN)-code.zip has 256 or more files in it which may cause submission problems"; fi

.PHONY: all clean

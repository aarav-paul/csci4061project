#include "../../include/table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char LOCALHOST[IP_LEN] = "127.0.0.1";
char TABLE_FILE_PATH[MAX_PATH] = "./test_cases/resources/0.tbl";

// Checks that all keys of table exist in other with the same values
// and that all keys of other exist in table with the same values
int table_equal(table_t *table, table_t *other) {
    bucket_t *ptr;
    bucket_t *other_ptr;

    for (int i = 0; i < TABLE_LEN; i++) {
        // check relation from table to other
        ptr = table->buckets[i];
        while (ptr != NULL) {
            if ((other_ptr = table_get(other, ptr->ip)) == NULL) {
                printf("could not find bucket in other table\n");
                return -1;
            }

            if (other_ptr->requests != ptr->requests) {
                printf("other table requests not euqal\n");
                return -1;
            }
            ptr = ptr->next;
        }

        // check relation from other to table
        ptr = other->buckets[i];
        while (ptr != NULL) {
            if ((other_ptr = table_get(table, ptr->ip)) == NULL) {
                printf("could not find bucket in other table reverse\n ip: %s\n", ptr->ip);
                return -1;
            }

            if (other_ptr->requests != ptr->requests) {
                printf("other table requests not euqal reverse\n");
                return -1;
            }
            ptr = ptr->next;
        }
    }

    return 0;
}

int test_empty_table_free() {
    table_t *table = table_init();
    table_free(table);
    return 0;
}

int test_empty_table_get() {
    table_t *table = table_init();

    if (table_get(table, LOCALHOST) != NULL) {
        printf("invalid table get did not return null\n");
        return -1;
    }

    table_free(table);
    return 0;
}

int test_table_add() {
    table_t *table = table_init();

    // add 5 buckets with unique IPs, no collisions
    for (int i = 0; i < 5; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);
    }

    table_free(table);
    return 0;
}

int test_table_add_collision() {
    table_t *table = table_init();

    // add 100 buckets with unique IPs, ensures collisions in hash table
    for (int i = 0; i < 100; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);
    }

    table_free(table);
    return 0;
}

int test_table_print() {
    table_t *table = table_init();

    for (int i = 0; i < 5; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);
        bucket->requests = i;
    }

    table_print(table);
    table_free(table);
    return 0;
}

int test_table_add_and_get() {
    table_t *table = table_init();

    // add 5 buckets AND check their pointers in the get method
    for (int i = 0; i < 5; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);

        bucket_t *other = table_get(table, bucket->ip);
        if (other == NULL) {
            printf("bucket pointer is null after valid table get\n");
            return -1;
        }
        if (other != bucket) {
            printf("bucket pointers do not match\n");
            table_free(table);
            return -1;
        }

        if (strcmp(other->ip, bucket->ip) != 0) {
            printf("bucket IPs do not match\n");
            table_free(table);
            return -1;
        }
    }

    table_free(table);
    return 0;
}

int test_table_add_and_get_collision() {
    table_t *table = table_init();

    // add 100 buckets AND check their pointers in the get method
    for (int i = 0; i < 100; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);

        bucket_t *other = table_get(table, bucket->ip);
        if (other == NULL) {
            printf("null ptr value from valid table get\n");
            return -1;
        }

        if (other != bucket) {
            printf("bucket pointers do not match\n");
            table_free(table);
            return -1;
        }

        if (strcmp(other->ip, bucket->ip) != 0) {
            printf("bucket IPs do not match\n");
            table_free(table);
            return -1;
        }
    }

    table_free(table);
    return 0;
}

int test_table_to_file() {
    table_t *table = table_init();
    for (int i = 0; i < 100; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        table_add(table, bucket);
    }

    int res = table_to_file(table, TABLE_FILE_PATH);
    table_free(table);

    if (res == -1) {
        printf("failed to write table to file\n");
    }

    return res;
}

int test_table_from_file() {
    table_t *table = table_init();
    for (int i = 0; i < 100; i++) {
        char ip_buf[IP_LEN];
        sprintf(ip_buf, "127.0.0.%d", i);

        bucket_t *bucket = bucket_init(ip_buf);
        bucket->requests = i;
        table_add(table, bucket);
    }

    if (table_to_file(table, TABLE_FILE_PATH) < 0) {
        printf("failed to write table to file\n");
        table_free(table);
        return -1;
    }

    table_t *other = table_from_file(TABLE_FILE_PATH);
    if (other == NULL) {
        printf("failed to read table from file\n");
        table_free(table);
        return -1;
    }

    int res = table_equal(table, other);
    table_free(table);
    table_free(other);

    if (res != 0) {
        printf("original table and table read from file are not equal\n");
    }

    return res;
}

int print_table_path(const char file_path[MAX_PATH]) {
    table_t* table = table_from_file(file_path);
    if (table == NULL) {
        return -1;
    }

    table_print(table);
    table_free(table);
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc == 1) {
        printf("table_test requires an argument for which test you want to run\n");
        printf("usage: table_test <testcase>\n");
        return -1;
    }

    if (strcmp(argv[1], "empty_table_free") == 0) {
        if (test_empty_table_free() != 0) {
            printf("test failed: empty_table_free\n");
            return -1;
        }
    } else if (strcmp(argv[1], "empty_table_get") == 0) {
        if (test_empty_table_get() != 0) {
            printf("test failed: empty_table_get\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_add") == 0) {
        if (test_table_add() != 0) {
            printf("test failed: table_add\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_add_collision") == 0) {
        if (test_table_add_collision() != 0) {
            printf("test failed: table_add\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_print") == 0) {
        if (test_table_print() != 0) {
            printf("test failed: table_print\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_add_and_get") == 0) {
        if (test_table_add_and_get() != 0) {
            printf("test failed: table_add_and_get\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_add_and_get_collision") == 0) {
        if (test_table_add_and_get_collision() != 0) {
            printf("test failed: table_add_and_get\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_to_file") == 0) {
        if (test_table_to_file() != 0) {
            printf("test failed: table_to_file\n");
            return -1;
        }
    } else if (strcmp(argv[1], "table_from_file") == 0) {
        if (test_table_from_file() != 0) {
            printf("test failed: table_from_file\n");
            return -1;
        }
    } else if (strcmp(argv[1], "print_table_path") == 0) {
        if (argc < 3 || strlen(argv[2]) < 3) {
            printf("invalid arguments to print_table_path\n");
            return -1;
        }

        if (print_table_path(argv[2]) != 0) {
            printf("test failed: print_table_path\n");
            return -1;
        }

        return 0;
    } 

    printf("test passed\n");
    return 0;
}

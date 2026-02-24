#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "./include/map.h"

bucket_t *bucket_init(const char ip[IP_LEN]) {
    return NULL;
}

table_t *table_init() {
    return NULL;
}

void table_print(const table_t *table) {
    return;
}

void table_free(table_t *table) {
    return;
}

int table_add(table_t *table, bucket_t *bucket) {
    return 0;
}

bucket_t *table_get(table_t *table, const char ip[IP_LEN]) {
    return NULL;
}

int hash_ip(const char ip[IP_LEN]) {
    return 0;
}

int table_to_file(table_t *table, const char out_file[MAX_PATH]) {
    return 0;
}

table_t *table_from_file(const char in_file[MAX_PATH]) {
    return NULL;
}

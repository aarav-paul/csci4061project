#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "./include/map.h"

bucket_t *bucket_init(const char ip[IP_LEN]) {
    if (ip == NULL) {
        return NULL;
    }

    bucket_t *bucket = malloc(sizeof(bucket_t));
    if (bucket == NULL) {
        return NULL;
    }
    memset(bucket, 0, sizeof(bucket_t));
    strncpy(bucket->ip, ip, IP_LEN - 1);
    bucket->ip[IP_LEN - 1] = '\0';
    bucket->requests = 0;
    bucket->next = NULL;
    return bucket;
}
table_t *table_init() {
    table_t *table = calloc(1, sizeof(table_t));
    return table;
}


void table_print(const table_t *table) {
    if (table == NULL) {return;}

    for (int i = 0; i < TABLE_LEN; i++) {
        bucket_t *curr = table->buckets[i];
        while (curr != NULL) {
            printf("%s - %d\n", curr->ip, curr->requests);
            curr = curr->next;
        }
    }
}
void table_free(table_t *table) {
    if (table == NULL) { return; }
    for (int i = 0; i < TABLE_LEN; i++) {
        bucket_t *curr = table->buckets[i];
        while (curr != NULL) {
            bucket_t *next = curr->next;
            free(curr);
            curr = next;
        }
        table->buckets[i] = NULL;
    }
    free(table);
}
int table_add(table_t *table, bucket_t *bucket) {
    if (table == NULL || bucket == NULL || bucket->ip[0] == '\0') {
        return -1;
    }
    int idx = hash_ip(bucket->ip);
    if (idx < 0 || idx >= TABLE_LEN) {
        return -1;
    }
    bucket->next = table->buckets[idx];
    table->buckets[idx] = bucket;
    return 0;
}
bucket_t *table_get(table_t *table, const char ip[IP_LEN]) {
    if (table == NULL || ip == NULL) {
        return NULL;
    }
    int idx = hash_ip(ip);
    if (idx < 0 || idx >= TABLE_LEN) {
        return NULL;
    }
    bucket_t *curr = table->buckets[idx];
    while (curr != NULL) {
        if (strcmp(curr->ip, ip) == 0) {
            return curr;
        }
        curr = curr->next;
    }
    return NULL;
}
int hash_ip(const char ip[IP_LEN]) {
    if (ip == NULL) {
        return -1;
    }
    int sum = 0;
    for (int i = 0; i < IP_LEN && ip[i] != '\0'; i++) {
        sum += (unsigned char)ip[i];
    }
    if (TABLE_LEN <= 0) {
        return -1;
    }
    return sum % TABLE_LEN;
}
int table_to_file(table_t *table, const char out_file[MAX_PATH]) {
    if (table == NULL || out_file == NULL) {
        return -1;
    }
    FILE *fp = fopen(out_file, "wb");
    if (fp == NULL) {
        perror("fopen");
        return -1;
    }
    for (int i = 0; i < TABLE_LEN; i++) {
        bucket_t *curr = table->buckets[i];
        while (curr != NULL) {
            if (fwrite(curr, sizeof(bucket_t), 1, fp) != 1) {
                perror("fwrite");
                fclose(fp);
                return -1;
            }
            curr = curr->next;
        }
    }
    if (fclose(fp) != 0) {
        perror("fclose");
        return -1;
    }
    return 0;
}


table_t *table_from_file(const char in_file[MAX_PATH]) {
    if (in_file == NULL) {
        return NULL;
    }
    FILE *fp = fopen(in_file, "rb");
    if (fp == NULL) {
        perror("fopen");
        return NULL;
    }
    table_t *table = table_init();
    if (table == NULL) {
        fclose(fp);
        return NULL;
    }
    bucket_t tmp;
    while (fread(&tmp, sizeof(bucket_t), 1, fp) == 1) {
        bucket_t *bucket = bucket_init(tmp.ip);
        if (bucket == NULL) {
            table_free(table);
            fclose(fp);
            return NULL;
        }
        bucket->requests = tmp.requests;
        if (table_add(table, bucket) != 0) {
            free(bucket);
            table_free(table);
            fclose(fp);
            return NULL;
        }
    }
    if (ferror(fp)) {
        perror("fread");
        table_free(table);
        fclose(fp);
        return NULL;
    }
    if (fclose(fp) != 0) {
        perror("fclose");
        table_free(table);
        return NULL;
    }
    return table;
}

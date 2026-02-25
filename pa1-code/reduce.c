#include "./include/reduce.h"

#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "./include/table.h"

int main(int argc, char *argv[]) {
    if (argc != 5) {
        printf("Usage: reduce <read dir> <out file> <start ip> <end ip>\n");
        return 1;
    }

    char *dir_name = argv[1];
    char *outfile = argv[2];
    char *start_str = argv[3];
    char *end_str = argv[4];

    if (start_str[0] == '\0' || end_str[0] == '\0') {
        printf("reduce: invalid IP range\n");
        return 1;
    }

    for (int i = 0; start_str[i] != '\0'; i++) {
        if (!isdigit(start_str[i])) {
            printf("reduce: invalid IP range\n");
            return 1;
        }
    }

    for (int i = 0; end_str[i] != '\0'; i++) {
        if (!isdigit(end_str[i])) {
            printf("reduce: invalid IP range\n");
            return 1;
        }
    }

    int start = atoi(start_str);
    int end = atoi(end_str);
    table_t *table = table_init();

    if (table == NULL) {
        return 1;
    }

    DIR *dir = opendir(dir_name);

    if (dir == NULL) {
        perror("opendir");
        table_free(table);
        return 1;
    }

    struct dirent *file;

    while ((file = readdir(dir)) != NULL) {
        if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
            continue;
        }

        char path[MAX_PATH];
        snprintf(path, MAX_PATH, "%s/%s", dir_name, file->d_name);

        if (reduce_file(table, path, start, end) != 0) {
            closedir(dir);
            table_free(table);
            return 1;
        }
    }

    closedir(dir);

    if (table_to_file(table, outfile) != 0) {
        table_free(table);
        return 1;
    }

    table_free(table);
    return 0;
}

int reduce_file(table_t *table, const char file_path[MAX_PATH], const int start,
                const int end) {
    table_t *temp = table_from_file(file_path);

    if (temp == NULL) {
        return -1;
    }

    for (int i = 0; i < TABLE_LEN; i++) {
        bucket_t *bucket = temp->buckets[i];

        while (bucket != NULL) {
            int byte = atoi(bucket->ip);

            if (byte >= start && byte < end) {
                bucket_t *match = table_get(table, bucket->ip);

                if (match != NULL) {
                    match->requests += bucket->requests;

                } else {
                    bucket_t *new_bucket = bucket_init(bucket->ip);

                    if (new_bucket == NULL) {
                        table_free(temp);
                        return -1;
                    }
                    new_bucket->requests = bucket->requests;

                    if (table_add(table, new_bucket) != 0) {
                        free(new_bucket);
                        table_free(temp);
                        return -1;
                    }
                }
            }

            bucket = bucket->next;
        }
    }

    table_free(temp);
    return 0;
}
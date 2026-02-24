#include "./include/map.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "./include/table.h"

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: map <outfile> <infiles...>\n");
        return 1;
    }

    const char *out_file = argv[1];

    table_t *table = table_init();
    if (table == NULL) {
        perror("table_init");
        return 1;
    }

    for (int i = 2; i < argc; i++) {
        if (map_log(table, argv[i]) != 0) {
            table_free(table);
            return 1;
        }
    }

    if (table_to_file(table, out_file) != 0) {
        table_free(table);
        return 1;
    }

    table_free(table);
    return 0;
}

int map_log(table_t *table, const char file_path[MAX_PATH]) {
    if (table == NULL || file_path == NULL) {
        return 1;
    }
    FILE *fp = fopen(file_path, "r");
    if (fp == NULL) {
        perror("fopen");
        return 1;
    }
    char line[256];
    while (fgets(line, sizeof(line), fp) != NULL) {
        log_line_t log = {0};
        int parsed = sscanf(line, "%19[^,],%15[^,],%7[^,],%36[^,],%3s",
                            log.timestamp, log.ip, log.method, log.route,
                            log.status);
        if (parsed != 5) {
            continue;
        }
        bucket_t *bucket = table_get(table, log.ip);
        if (bucket == NULL) {
            bucket = bucket_init(log.ip);
            if (bucket == NULL) {
                fclose(fp);
                return 1;
            }
            if (table_add(table, bucket) != 0) {
                free(bucket);
                fclose(fp);
                return 1;
            }
        }
        bucket->requests++;
    }
    if (ferror(fp)) {
        fclose(fp);
        return 1;
    }
    if (fclose(fp) != 0) {
        return 1;
    }
    return 0;
}

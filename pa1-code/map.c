#include "./include/map.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: map <outfile> <infiles...>\n");
    return EXIT_FAILURE;
  }

  const char *output_table = argv[1];

  table_t *table = table_init();
  if (!table) {
    fprintf(stderr, "Failed to initialize table\n");
    return EXIT_FAILURE;
  }

  for (int i = 2; i < argc; i++) {
    if (map_log(table, argv[i]) != 0) {
      fprintf(stderr, "Failed to map log file: %s\n", argv[i]);
      table_free(table);
      return EXIT_FAILURE;
    }
  }

  if (table_to_file(table, output_table) != 0) {
    fprintf(stderr, "Failed to save table to file: %s\n", output_table);
    table_free(table);
    return EXIT_FAILURE;
  }

  table_free(table);
  return EXIT_SUCCESS;
}

int map_log(table_t *table, const char file_path[MAX_PATH]) {
  if (!table || !file_path)
    return -1;

  FILE *fp = fopen(file_path, "r");
  if (!fp) {
    perror("fopen");
    return -1;
  }

  char line[1024];

  char timestamp[64];
  char ip[IP_LEN];
  char method[8];
  char path[512];
  char status[4];

  while (fgets(line, sizeof(line), fp)) {

    line[strcspn(line, "\r\n")] = '\0';

    char *p = line;
    while (*p == ' ' || *p == '\t')
      p++;

    char *last = strrchr(p, ',');
    if (!last)
      continue;

    strncpy(status, last + 1, sizeof(status) - 1);
    status[sizeof(status) - 1] = '\0';

    *last = '\0';

    if (sscanf(p, "%63[^,],%15[^,],%7[^,],%511[^\n]", timestamp, ip, method,
               path) != 4) {
      continue;
    }

    bucket_t *bucket = table_get(table, ip);
    if (bucket) {
      bucket->requests += 1;
    } else {
      bucket = bucket_init(ip);
      if (!bucket)
        continue;
      bucket->requests = 1;
      table_add(table, bucket);
    }
  }

  fclose(fp);
  return 0;
}

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#include "./include/table.h"

#define MAX_FILES 1024
#define MAX_PATH 1024

struct record {
  char ip[IP_LEN];
  int requests;
};

int cmp_record(const void *a, const void *b) {
  const struct record *ra = a;
  const struct record *rb = b;
  return strcmp(ra->ip, rb->ip);
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage: mapreduce <directory> <n mappers> <n reducers>\n");
    return 1;
  }

  char *dir_name = argv[1];
  int n_mappers = atoi(argv[2]);
  int n_reducers = atoi(argv[3]);

  if (n_mappers < 1 || n_reducers < 1) {
    fprintf(stderr, "mapreduce: cannot have less than one mapper or reducer\n");
    return 1;
  }

  DIR *dir = opendir(dir_name);
  if (!dir) {
    perror("opendir");
    return 1;
  }

  char *files[MAX_FILES];
  int file_count = 0;
  struct dirent *entry;

  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    files[file_count] = malloc(MAX_PATH);
    if (!files[file_count]) {
      fprintf(stderr, "malloc failed\n");
      closedir(dir);
      return 1;
    }
    snprintf(files[file_count], MAX_PATH, "%s/%s", dir_name, entry->d_name);
    file_count++;
    if (file_count >= MAX_FILES)
      break;
  }
  closedir(dir);

  if (file_count == 0) {
    fprintf(stderr, "No files found in directory.\n");
    return 1;
  }

  if (mkdir("./intermediate", 0777) < 0 && errno != EEXIST) {
    perror("mkdir intermediate");
    return 1;
  }

  int files_per_mapper = file_count / n_mappers;
  int remainder = file_count % n_mappers;
  int file_index = 0;

  pid_t mapper_pids[n_mappers];

  for (int i = 0; i < n_mappers; i++) {
    int count = files_per_mapper + (i < remainder ? 1 : 0);

    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      return 1;
    }

    if (pid == 0) {
      char outfile[MAX_PATH];
      snprintf(outfile, sizeof(outfile), "./intermediate/%d.tbl", i);

      char *args[count + 3];
      args[0] = "./map";
      args[1] = outfile;

      for (int j = 0; j < count; j++) {
        args[j + 2] = files[file_index + j];
      }
      args[count + 2] = NULL;

      execv("./map", args);
      perror("execv failed");
      exit(1);
    } else {
      mapper_pids[i] = pid;
      file_index += count;
    }
  }

  for (int i = 0; i < n_mappers; i++) {
    int status;
    waitpid(mapper_pids[i], &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      fprintf(stderr, "mapreduce: mapper %d failed\n", i);
      return 1;
    }
  }

  int range_per_reducer = 256 / n_reducers;
  int range_remainder = 256 % n_reducers;

  pid_t reducer_pids[n_reducers];

  for (int i = 0; i < n_reducers; i++) {
    int start_ip =
        i * range_per_reducer + (i < range_remainder ? i : range_remainder);
    int end_ip = start_ip + range_per_reducer + (i < range_remainder ? 1 : 0);

    pid_t pid = fork();
    if (pid < 0) {
      perror("fork");
      return 1;
    }

    if (pid == 0) {
      char outfile[MAX_PATH];
      snprintf(outfile, sizeof(outfile), "./out/%d.tbl", i);

      char start_str[4], end_str[4];
      snprintf(start_str, sizeof(start_str), "%d", start_ip);
      snprintf(end_str, sizeof(end_str), "%d", end_ip);

      char *args[] = {"./reduce", "./intermediate", outfile,
                      start_str,  end_str,          NULL};
      execv("./reduce", args);
      perror("execv failed");
      exit(1);
    } else {
      reducer_pids[i] = pid;
    }
  }

  for (int i = 0; i < n_reducers; i++) {
    int status;
    waitpid(reducer_pids[i], &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      fprintf(stderr, "mapreduce: reducer %d failed\n", i);
      return 1;
    }
  }

  table_t *global = table_init();
  if (!global) {
    fprintf(stderr, "Failed to init global table\n");
    return 1;
  }

  dir = opendir("./out");
  if (!dir) {
    perror("opendir out");
    table_free(global);
    return 1;
  }

  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;

    char path[MAX_PATH];
    snprintf(path, MAX_PATH, "./out/%s", entry->d_name);

    table_t *t = table_from_file(path);
    if (!t) {
      fprintf(stderr, "Failed to load table from %s\n", path);
      continue;
    }

    for (int i = 0; i < TABLE_LEN; i++) {
      bucket_t *b = t->buckets[i];
      while (b) {
        bucket_t *g = table_get(global, b->ip);
        if (g) {
          g->requests += b->requests;
        } else {
          bucket_t *nb = bucket_init(b->ip);
          if (!nb) {
            table_free(t);
            closedir(dir);
            table_free(global);
            return 1;
          }
          nb->requests = b->requests;
          if (table_add(global, nb) != 0) {
            free(nb);
            table_free(t);
            closedir(dir);
            table_free(global);
            return 1;
          }
        }
        b = b->next;
      }
    }
    table_free(t);
  }
  closedir(dir);

  int count = 0;
  for (int i = 0; i < TABLE_LEN; i++) {
    bucket_t *b = global->buckets[i];
    while (b) {
      count++;
      b = b->next;
    }
  }

  struct record *arr = malloc(sizeof(struct record) * count);
  if (!arr) {
    fprintf(stderr, "malloc failed\n");
    table_free(global);
    return 1;
  }

  int idx = 0;
  for (int i = 0; i < TABLE_LEN; i++) {
    bucket_t *b = global->buckets[i];
    while (b) {
      strncpy(arr[idx].ip, b->ip, IP_LEN);
      arr[idx].ip[IP_LEN - 1] = '\0';
      arr[idx].requests = b->requests;
      idx++;
      b = b->next;
    }
  }

  qsort(arr, count, sizeof(struct record), cmp_record);

  for (int i = 0; i < count; i++) {
    printf("%s - %d\n", arr[i].ip, arr[i].requests);
  }

  free(arr);
  table_free(global);

  for (int i = 0; i < file_count; i++)
    free(files[i]);

  return 0;
}

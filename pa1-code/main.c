#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_FILES 1024
#define MAX_PATH 512

int main(int argc, char *argv[]) {
  if (argc != 4) {
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

  // Read all files into array
  char *files[MAX_FILES];
  int file_count = 0;
  struct dirent *entry;
  while ((entry = readdir(dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    if (file_count >= MAX_FILES)
      break;
    files[file_count] = malloc(MAX_PATH);
    snprintf(files[file_count], MAX_PATH, "%s/%s", dir_name, entry->d_name);
    file_count++;
  }
  closedir(dir);

  if (file_count == 0) {
    fprintf(stderr, "No files found in directory\n");
    return 1;
  }

  // Split files among mappers
  int base_files = file_count / n_mappers;
  int extra = file_count % n_mappers;
  int start = 0;
  pid_t map_pids[n_mappers];

  for (int i = 0; i < n_mappers; i++) {
    int count = base_files + (i < extra ? 1 : 0);

    pid_t pid = fork();
    if (pid == 0) { // child
      char outfile[MAX_PATH];
      snprintf(outfile, MAX_PATH, "./intermediate/%d.tbl", i);

      // Build args array for exec
      char *args[count + 3]; // ./map, outfile, files..., NULL
      args[0] = "./map";
      args[1] = outfile;
      for (int j = 0; j < count; j++)
        args[j + 2] = files[start + j];
      args[count + 2] = NULL;

      execvp(args[0], args);
      perror("execvp");
      exit(1);
    } else if (pid > 0) { // parent
      map_pids[i] = pid;
    } else {
      perror("fork");
      return 1;
    }
    start += count;
  }

  // Wait for all mappers
  for (int i = 0; i < n_mappers; i++) {
    int status;
    if (waitpid(map_pids[i], &status, 0) == -1) {
      perror("waitpid");
      return 1;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      fprintf(stderr, "Mapper %d exited with error\n", i);
      return 1;
    }
  }

  // Split IP key space for reducers
  int keys_per = 256 / n_reducers;
  int remainder = 256 % n_reducers;
  int key_start = 0;
  pid_t reduce_pids[n_reducers];

  for (int i = 0; i < n_reducers; i++) {
    int key_end = key_start + keys_per + (i < remainder ? 1 : 0);

    pid_t pid = fork();
    if (pid == 0) { // child
      char outfile[MAX_PATH];
      snprintf(outfile, MAX_PATH, "./out/%d.tbl", i);

      char start_ip[4], end_ip[4];
      snprintf(start_ip, 4, "%d", key_start);
      snprintf(end_ip, 4, "%d", key_end);

      char *args[] = {"./reduce", "./intermediate", outfile,
                      start_ip,   end_ip,           NULL};
      execvp(args[0], args);
      perror("execvp");
      exit(1);
    } else if (pid > 0) { // parent
      reduce_pids[i] = pid;
    } else {
      perror("fork");
      return 1;
    }

    key_start = key_end;
  }

  // Wait for all reducers
  for (int i = 0; i < n_reducers; i++) {
    int status;
    if (waitpid(reduce_pids[i], &status, 0) == -1) {
      perror("waitpid");
      return 1;
    }
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
      fprintf(stderr, "Reducer %d exited with error\n", i);
      return 1;
    }
  }

  // Print out all reducer outputs
  DIR *out_dir = opendir("./out");
  if (!out_dir) {
    perror("opendir");
    return 1;
  }
  while ((entry = readdir(out_dir)) != NULL) {
    if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
      continue;
    char path[MAX_PATH];
    snprintf(path, MAX_PATH, "./out/%s", entry->d_name);
    FILE *f = fopen(path, "r");
    if (!f)
      continue;
    char buf[512];
    while (fgets(buf, sizeof(buf), f)) {
      printf("%s", buf);
    }
    fclose(f);
  }
  closedir(out_dir);

  // Free allocated memory
  for (int i = 0; i < file_count; i++)
    free(files[i]);

  return 0;
}

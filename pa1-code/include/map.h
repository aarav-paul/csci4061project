#ifndef MAP_H
#define MAP_H

#include "./table.h"

// update with what log lines have
typedef struct log_line {
    char route[37];
    char timestamp[20];
    char ip[IP_LEN];
    char method[8];
    char status[4];
} log_line_t;

// The main driver of the mappers
//
// Read all files and map user requests
int map_log(table_t* table, const char file_path[MAX_PATH]);

#endif // MAP_H
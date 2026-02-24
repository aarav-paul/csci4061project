#ifndef REDUCE_H
#define REDUCE_H

#include "./table.h"

int reduce_file(table_t *table, const char file_path[MAX_PATH], const int start_ip,
                const int end_ip);

#endif    // REDUCE_H

#ifndef TABLE_H
#define TABLE_H

#define MAX_PATH 255    // max path length
#define TABLE_LEN 17    // keep hash table array length a prime number for better hashing
#define IP_LEN 16       // max ip length including null terminator

// Definition of a "bucket" node in a hash table
//
// Essentially, this is a linked list that resides at every index of the hash table
typedef struct bucket {
    struct bucket *next;
    char ip[IP_LEN];
    int requests;
} bucket_t;

// Definition of table
typedef struct table {
    bucket_t *buckets[TABLE_LEN];
} table_t;

// Allocate a bucket and copy the IP into it
//
// Return the bucket on success, NULL on failure, or if ip is NULL
bucket_t *bucket_init(const char ip[IP_LEN]);

// Allocate a table and initialize the memory to 0
//
// The memory must be initialized to 0, since referencing any
// of the bucket pointers could otherwise result in some
// non NULL garbage value
table_t *table_init();

// Print the contents of a table
//
// The order of which you print the items does not matter,
// as they will be piped into the "sort" command for grading
//
// Print format: {IP} - {num requests}
void table_print(const table_t *table);

// Free a table and all corresponding buckets
void table_free(table_t *table);

// Add the given bucket to the table.
// If there is already a bucket corresponding to the IP,
// do not merge the two, as this function should only be called
// if table_get first returns NULL.
//
// This function will fail if:
// - table is NULL
// - bucket is NULL or has no IP to hash
//
// Return 0 on success and -1 on failure
int table_add(table_t *table, bucket_t *bucket);

// Returns the hash table bucket corresponding to the given IP
//
// This function will fail if:
// - table is NULL
// - ip is NULL
//
// Returns NULL on failure, or if the requested bucket does not exist
bucket_t *table_get(table_t *table, const char ip[IP_LEN]);

// Return the hash table idx of the given ip
//
// The hash function should be deterministic, meaning that
// every hash of a given IP should result in the same index.
//
// An example of a simple hash function would be:
// add the int values of every character in the ip, and mod by table length
//
// When computing the hash, IPs can be less than IP_LENGTH.
// Make sure that you do not hash any character values past the
// end of the string (\0), or your hash function will not be deterministic.
//
// This function will fail if ip is NULL, return -1 on failure
int hash_ip(const char ip[IP_LEN]);

// Write the given table to a file. To write to a table file,
// write every bucket in the table sequentially with the
// `fwrite` function (in binary). Your solution must follow
// this standard, otherwise it will fail the autograder.
//
// The file extension should be .tbl (representing a hash table)
// You do not need to check this, but it is required to pass tests
//
// [See here on how to write/read structs to a
// file](https://www.geeksforgeeks.org/c/read-write-structure-from-to-a-file-in-c/)
//
// This function will fail if:
// - table is NULL
// - out_file is NULL
// - file I/O fails
//
// Return 0 on success, -1 on failure
int table_to_file(table_t *table, const char out_file[MAX_PATH]);

// Read the requested file and parse it into a hash table.
// The file must be a binary list of bucket_t structs
// to read in sequence and add to a newly allocated table.
// Your solution must follow this standard, otherwiswe it will
// fail the autograder.
//
// [See here on how to write/read structs to a
// file](https://www.geeksforgeeks.org/c/read-write-structure-from-to-a-file-in-c/)
//
// This function will fail if:
// - table is NULL
// - in_file is NULL
// - file I/O fails
//
// Return a newly allocated table object on success, NULL on failure
table_t *table_from_file(const char in_file[MAX_PATH]);

#endif    // TABLE_H

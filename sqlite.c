#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
} Row;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + EMAIL_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE; // 293

const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t PAGE_SIZE = 4096;

typedef struct {
	int file_descriptor;
	uint32_t file_length;
	uint32_t num_pages;
	void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
	uint32_t root_page_num;
	Pager* pager;
} Table;

typedef struct {
	Table* table;
	uint32_t page_num;
	uint32_t cell_num;
	bool end_of_table;
} Cursor;

typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT,
	PREPARE_NEGATIVE_ID,
	PREPARE_STRING_TOO_LONG
} PrepareResult;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} StatementType;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct {
	StatementType type;
	Row row_to_insert;
} Statement;

typedef enum{
	// each node is one page
	NODE_INTERNAL,
	NODE_LEAF,
} NodeType;

// Common node header layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf node header mem format
//     byte 0      byte 1 - bool      byte 2-5             byte 6-9 
// NODE_TYPE_SIZE  IS_ROOT_SIZE  PARENT_POINTER_SIZE  LEAF_NODE_NUM_CELLS

// Leaft node body mem format
//    byte 10-13               byte 14-306            byte 307-310           byte 311-603
// LEAF_NODE_KEY(key 1)  LEAF_NODE_VALUE(byte 1)  LEAF_NODE_KEY(key 2)  LEAF_NODE_VALUE(value 2)

// Leaf node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE; // 293
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

uint32_t* leaf_node_num_cells(uint32_t* node) { 
	return node + LEAF_NODE_NUM_CELLS_OFFSET; 
}

uint32_t* leaf_node_cell(uint32_t* node, uint32_t cell_num) {
	return node + LEAF_NODE_HEADER_SIZE + (LEAF_NODE_CELL_SIZE * cell_num);
}

uint32_t* leaf_node_key(uint32_t* node, uint32_t cell_num) {
	return leaf_node_cell(node, cell_num);
}

uint32_t* leaf_node_value(uint32_t* node, uint32_t cell_num) {
	return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node) {
	*leaf_node_num_cells(node) = 0;
}

typedef struct {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
} InputBuffer;

InputBuffer* new_input_buffer() {
	InputBuffer* input_buffer = (InputBuffer*)malloc(sizeof(InputBuffer));
	input_buffer->buffer = NULL;
  	input_buffer->buffer_length = 0;
  	input_buffer->input_length = 0;

  	return input_buffer;
}

void close_input_buffer(InputBuffer* ib) {
	free(ib->buffer);
	free(ib);
}

void serialize_row(Row *r, void* dest) {
	memcpy(dest + ID_OFFSET, &(r->id), ID_SIZE);
	strncpy(dest + USERNAME_OFFSET, r->username, USERNAME_SIZE);
	strncpy(dest + EMAIL_OFFSET, r->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *r) {
	memcpy(&(r->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(r->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(r->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

Pager* pager_open(const char* filename) {
	int fd = open(filename,
		     O_RDWR | O_CREAT,
		     S_IWUSR | S_IRUSR
		);
	
	if (fd == -1) {
		printf("unable to open file\n");
		exit(EXIT_FAILURE);
	}

	off_t file_length = lseek(fd, 0, SEEK_END);

	Pager* pager = malloc(sizeof(Pager));
	pager->file_descriptor = fd;
	pager->file_length = file_length;
	pager->num_pages = file_length / PAGE_SIZE;

	if (file_length % PAGE_SIZE != 0) {
		printf("db file is not a whole number of pages. Corrupt file\n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) { 
		pager->pages[i] = NULL;
	}

	return pager;
}

void* get_page(Pager* pager, uint32_t page_num) {
	if (page_num > TABLE_MAX_PAGES) {
		printf("tried to fetch page number out of bounds. %d > %d", page_num, TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}

	if  (pager->pages[page_num] == NULL) {
		// cache miss
		void* page = malloc(PAGE_SIZE);
		uint32_t pages_qtd = pager->file_length / PAGE_SIZE;

		if (pager->file_length % PAGE_SIZE) {
			// save a partial page
			pages_qtd += 1;
		}

		if (page_num <= pages_qtd) {
			lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t pages_bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
			if (pages_bytes_read == -1) {
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;
		
		if (page_num >= pager->num_pages) {
			pager->num_pages++;;
		}
	}

	return pager->pages[page_num];
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
	void* node = get_page(cursor->table->pager, cursor->page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);

	if (num_cells >= LEAF_NODE_MAX_CELLS) {
		printf("Should implement split leaf node");
		exit(EXIT_FAILURE);
	}

	for (int i = num_cells; i > cursor->cell_num; i--) {
		memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
	}
		
	*(leaf_node_num_cells(node)) += 1;
	*(leaf_node_key(node, cursor->cell_num)) = key;
	serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void pager_flush(Pager* pager, uint32_t page_num) {
	if (pager->pages[page_num] == NULL) {
		printf("trying to flush null page\n");
		exit(EXIT_FAILURE);
	}

	off_t page_offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	if (page_offset == -1) {
		printf("error seeking %d\n", errno);
		exit(EXIT_FAILURE);
	}

        ssize_t res = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
	if (res == -1) {
		printf("error: %d::when try to flush page %d", errno, page_num);
		exit(EXIT_FAILURE);
	}
}

void* cursor_value(Cursor* cursor) {
	void* page = get_page(cursor->table->pager, cursor->page_num);
	return leaf_node_value(page, cursor->cell_num);
}

void print_row(Row r) {
	printf("(%d, %s, %s)\n", r.id, r.username, r.email);
}

Cursor* cursor_table_start(Table* table) {
	Cursor* cursor = malloc(sizeof(Table));
	cursor->table = table;
	cursor->page_num = table->root_page_num;
	cursor->cell_num = 0;

	void* root_node = get_page(table->pager, cursor->page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->end_of_table = num_cells == 0;
	
	return cursor;
}

Cursor* cursor_table_end(Table* table) {
	Cursor* cursor = malloc(sizeof(Table));
	cursor->table = table;
	cursor->page_num = table->root_page_num;
	
	void* root_node = get_page(table->pager, cursor->page_num);
	uint32_t num_cells = *leaf_node_num_cells(root_node);
	cursor->cell_num = num_cells;
	cursor->end_of_table = true;
	
	return cursor;
}

void advance_cursor(Cursor* cursor) {
	uint32_t page_num = cursor->page_num;
	void* node = get_page(cursor->table->pager, page_num);

	cursor->cell_num++;
	if (cursor->cell_num >= (*leaf_node_num_cells(node))) { 
		cursor->end_of_table = true;
	}
}

void print_prompt() {
	printf("db > ");
}

Table* open_db(const char* filename) {
	Pager* pager = pager_open(filename);

	Table* table = (Table*)malloc(sizeof(Table));
	table->root_page_num = 0;
	table->pager = pager;

	if (pager->num_pages == 0) {
		// new database file, initialize page 0 as leaf node
		void* root_node = get_page(pager, 0);
		initialize_leaf_node(root_node);
	}

	return table;
}

void close_db(Table* table) {
	Pager* pager = table->pager;

	for (uint32_t i = 0; i < pager->num_pages; i++) {
		if (pager->pages[i] == NULL) {
			continue;
		}

		pager_flush(pager, i);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	if (close(pager->file_descriptor) == -1) {
		printf("error closing db file. \n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		free(pager->pages[i]);
		pager->pages[i]	= NULL;
	} 

	free(pager);
	free(table);
}

void read_input(InputBuffer *input_buffer) {
	ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

	if (bytes_read <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	input_buffer->input_length = bytes_read - 1;
	input_buffer->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer *ib, Table *table) {
	if (strcmp(ib->buffer, ".exit") == 0) {
		close_input_buffer(ib);
		close_db(table);
		exit(EXIT_SUCCESS);
		return META_COMMAND_SUCCESS;
	}

	return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_insert(InputBuffer *ib, Statement *statement) {
	statement->type = STATEMENT_INSERT;

	char *insert_keyword = strtok(ib->buffer, " ");
	char *id_str = strtok(NULL, " ");
	char *username = strtok(NULL, " ");
	char *email = strtok(NULL, " ");

	if (id_str == NULL || username == NULL || email == NULL) {
		return PREPARE_SYNTAX_ERROR;
	}

	if (strlen(username) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}

	int id = atoi(id_str);
	if (id < 0) {
		return PREPARE_NEGATIVE_ID;
	}

	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *ib, Statement *statement) {
	if (strcmp(ib->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	if (strncmp(ib->buffer, "insert", 6) == 0) {
		return prepare_insert(ib, statement);
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *st, Table *table) {
	void* node = get_page(table->pager, table->root_page_num);
	if (*(leaf_node_num_cells(node)) >= LEAF_NODE_MAX_CELLS) {
		return EXECUTE_TABLE_FULL;
	}
	
	Cursor* cursor = cursor_table_end(table);
	Row* r = &(st->row_to_insert);

	leaf_node_insert(cursor, r->id, r);

	free(cursor);

	return EXECUTE_SUCCESS; 
}

ExecuteResult execute_select(Statement *st, Table *table) {
	Row r;
	Cursor* cursor = cursor_table_start(table);

	while(!cursor->end_of_table) {
		deserialize_row(cursor_value(cursor), &r);
		advance_cursor(cursor);
		print_row(r);
	}

	free(cursor);

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *st, Table *table) {
	switch(st->type) {
		case (STATEMENT_SELECT):
			return execute_select(st, table);
		case (STATEMENT_INSERT):
			return execute_insert(st, table);
	}
}

int main(int argc, char *argv[]) {
	if (argc < 2) {
		printf("must suply a database filename\n"); 
		exit(EXIT_FAILURE);
	}

	char* filename = argv[1];
	Table* table = open_db(filename);

	InputBuffer* input_buffer = new_input_buffer();
	
	while(true) {
		print_prompt();
		read_input(input_buffer);

		// Non-Sql statements 'meta-commands'
		if (input_buffer->buffer[0] == '.') {
			switch (do_meta_command(input_buffer, table)) {
				case (META_COMMAND_SUCCESS):
					continue;
				case (META_COMMAND_UNRECOGNIZED_COMMAND):
					printf("Unrecognized command '%s' \n", input_buffer->buffer);
					continue;
			}
		}

		Statement statement;
		switch (prepare_statement(input_buffer, &statement)) {
			case (PREPARE_SUCCESS):
				break;
			case (PREPARE_STRING_TOO_LONG):
				printf("string is too long\n");
				continue;
			case (PREPARE_NEGATIVE_ID):
				printf("ID must be positive\n");
				continue;
			case (PREPARE_SYNTAX_ERROR):
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
				continue;
		}

		switch (execute_statement(&statement, table)) {
			case (EXECUTE_SUCCESS):
				printf("executed\n");
				break;
			case (EXECUTE_TABLE_FULL):
				printf("Error: table is full\n");
				break;
		}
	}
}



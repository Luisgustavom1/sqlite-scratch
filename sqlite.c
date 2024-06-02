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
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
	int file_descriptor;
	uint32_t file_length;
	void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
	uint32_t num_rows;
	Pager* pager;
} Table;

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
	memcpy(dest + USERNAME_OFFSET, &(r->username), USERNAME_SIZE);
	memcpy(dest + EMAIL_OFFSET, &(r->email), EMAIL_SIZE);
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
	}

	return pager->pages[page_num];
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
	if (pager->pages[page_num] == NULL) {
		printf("trying to flush null page\n");
		exit(EXIT_FAILURE);
	}

	off_t page_offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	if (page_offset == -1) {
		printf("error seeking %d\n", errno);
		exit(EXIT_FAILURE);
	}

        ssize_t res = write(pager->file_descriptor, pager->pages[page_num], size);
	if (res == -1) {
		printf("error: %d::when try to flush page %d", errno, page_num);
		exit(EXIT_FAILURE);
	}
}

void* row_slot(Table* table, uint32_t row_num) {
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void* page = get_page(table->pager, page_num);
	uint32_t row_num_in_page = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_num_in_page * ROW_SIZE;
	return page + byte_offset;
}

void print_row(Row r) {
	printf("(%d, %s, %s)\n", r.id, r.username, r.email);
}

void print_prompt() {
	printf("db > ");
}

Table* open_db(const char* filename) {
	Table* table = (Table*)malloc(sizeof(Table));
	Pager* pager = pager_open(filename);
	
	table->num_rows = pager->file_length / ROW_SIZE;
	table->pager = pager;

	return table;
}

void close_db(Table* table) {
	Pager* pager = table->pager;
	uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

	for (uint32_t i = 0; i < num_full_pages; i++) {
		if (pager->pages[i] == NULL) {
			continue;
		}

		pager_flush(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
	uint32_t partial_page_num = num_full_pages;
	if (num_additional_rows > 0 && pager->pages[partial_page_num] != NULL) {
		pager_flush(pager, partial_page_num, num_additional_rows * ROW_SIZE);
		free(pager->pages[partial_page_num]);
		pager->pages[partial_page_num] = NULL;
	}

	if (close(pager->file_descriptor) == -1) {
		printf("error closing db file. \n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = partial_page_num + 1; i < TABLE_MAX_PAGES; i++) {
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
	if (table->num_rows >= TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}

	Row *r = &(st->row_to_insert);
	serialize_row(r, row_slot(table, table->num_rows));
	table->num_rows++;

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *st, Table *table) {
	Row r;

	for (uint32_t i = 0; i < table->num_rows; i++) {
		deserialize_row(row_slot(table, i), &r);
		print_row(r);
	}

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



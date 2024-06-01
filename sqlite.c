#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} StatementType;

typedef struct {
	StatementType type;
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

void print_prompt() {
	printf("db > ");
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

MetaCommandResult do_meta_command(InputBuffer* ib) {
	if (strcmp(ib->buffer, ".exit") == 0) {
		close_input_buffer(ib);
		exit(EXIT_SUCCESS);
		return META_COMMAND_SUCCESS;
	}

	return META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(InputBuffer *ib, Statement *statement) {
	if (strncmp(ib->buffer, "select", 6) == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	if (strncmp(ib->buffer, "insert", 6) == 0) {
		statement->type = STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *st) {
	switch(st->type) {
		case (STATEMENT_SELECT):
			printf("select statement");
			break;
		case (STATEMENT_INSERT):
			printf("insert statement");
			break;
	}
}

int main(int argc, char *argv[]) {
	InputBuffer* input_buffer = new_input_buffer();

	while(true) {
		print_prompt();
		read_input(input_buffer);
	
		// Non-Sql statements 'meta-commands'
		if (input_buffer->buffer[0] == '.') {
			switch (do_meta_command(input_buffer)) {
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
			case (PREPARE_UNRECOGNIZED_STATEMENT):
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
				continue;
		}

		execute_statement(&statement);
		printf("Executed.\n");
	}
}


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
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
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
	EXECUTE_DUPLICATED_KEY
} ExecuteResult;

typedef struct {
	StatementType type;
	Row row_to_insert;
} Statement;

typedef enum {
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

// Leaf node header layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

// Leaf node body layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE; // 293
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2; // 7
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT; // 7

// Leaf node header mem format
//     byte 0      byte 1 - bool      byte 2-5             byte 6-9 					byte 10-13
// NODE_TYPE_SIZE  IS_ROOT_SIZE  PARENT_POINTER_SIZE  LEAF_NODE_NUM_CELLS  LEAF_NODE_NEXT_LEAF

// Leaf node body mem format
//    byte 10-13               byte 14-306            byte 307-310           byte 311-603
// LEAF_NODE_KEY(key 1)  LEAF_NODE_VALUE(byte 1)  LEAF_NODE_KEY(key 2)  LEAF_NODE_VALUE(value 2)

uint32_t* leaf_node_num_cells(void* node) { 
	return node + LEAF_NODE_NUM_CELLS_OFFSET; 
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
  return node + LEAF_NODE_HEADER_SIZE + (LEAF_NODE_CELL_SIZE * cell_num);
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
	return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num) {
	return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* leaf_node_next_leaf(void* node) {
	return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

NodeType get_node_type(void* node) {
	uint8_t value = *((uint8_t*)node + NODE_TYPE_OFFSET);
	return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
	uint8_t value = type;
	*((uint8_t*)node + NODE_TYPE_OFFSET) = value;
}

void set_node_root(void* node, bool is_root) {
	uint8_t value = is_root;
	*((uint8_t*)(node + IS_ROOT_OFFSET)) = true;
}

void initialize_leaf_node(void* node) {
	set_node_type(node, NODE_LEAF);
	set_node_root(node, false);
	*leaf_node_num_cells(node) = 0;
	*leaf_node_next_leaf(node) = 0; // leaf with no sibling
}

void indent(uint32_t level) {
	for (uint32_t i = 0; i < level; i++) {
		printf(" ");
	}
}

// Internal node header format
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

// Internal node body format
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

// Internal node body format
// Internal node header mem format
//     byte 0      byte 1 - bool      byte 2-5                 byte 6-9                      byte 10-13
// NODE_TYPE_SIZE  IS_ROOT_SIZE  PARENT_POINTER_SIZE  INTERNAL_NODE_NUM_KEYS_SIZE  INTERNAL_NODE_RIGHT_CHILD_SIZE

// Internal node body mem format
//       byte 14-17               byte 18-21                 byte 22-25             byte 26-29
// INTERNAL_NODE_CHILD_SIZE  INTERNAL_NODE_KEY_SIZE  INTERNAL_NODE_CHILD_SIZE INTERNAL_NODE_KEY_SIZE  
//       INTERNAL_NODE_CELL_SIZE (1)                        INTERNAL_NODE_CELL_SIZE (2)

uint32_t* internal_node_num_keys(void* node) {
	return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internal_node_right_child(void* node) {
	return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
	return node + INTERNAL_NODE_HEADER_SIZE + (INTERNAL_NODE_CELL_SIZE * cell_num);
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
	uint32_t num_keys = *internal_node_num_keys(node);
	if (child_num > num_keys) {
		printf("tried to access child_num %d > num_keys %d", child_num, num_keys);
		exit(EXIT_FAILURE);
	} else if (child_num == num_keys) {
		return internal_node_right_child(node);
	}
	return internal_node_cell(node, child_num);
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
	return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t* node_parent(void* node) {
	return node + PARENT_POINTER_OFFSET;
}

void initialize_internal_node(void* node) {
	set_node_type(node, NODE_INTERNAL);
	set_node_root(node, false);
	*internal_node_num_keys(node) = 0;
}

uint32_t get_node_max_key(void* node) {
	switch(get_node_type(node)) {
		case NODE_INTERNAL:
			return *internal_node_key(node, *internal_node_num_keys(node) - 1);
		case NODE_LEAF:
			return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
	}
}

bool is_node_root(void* node) {
	uint8_t isRoot = *((uint8_t*)(node + IS_ROOT_OFFSET));
	return (bool)isRoot;
}

void print_constants() {
	printf("ROW_SIZE: %d\n", ROW_SIZE);
	printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
	printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
	printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
	printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
	printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
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

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
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
      pager->num_pages = page_num + 1;
    }
  }

  return pager->pages[page_num];
}

uint32_t get_unused_page_num(Pager* pager) {
	return pager->num_pages;
} 

void create_new_root(Table* table, uint32_t right_child_page_num) {
	/*
	 * handle splitting the root.
	 * old root copied to new page, becomes left child.
	 * Address of right child passed in.
	 * Re-initialize root page to contain the new root node.
	 * New root node points to new children.
	*/
	void* root = get_page(table->pager, table->root_page_num);
	void* right_child = get_page(table->pager, right_child_page_num);
	uint32_t left_child_page_num = get_unused_page_num(table->pager);
	void* left_child = get_page(table->pager, left_child_page_num);

	/*
	 * copy old rot to left child
	*/
	memcpy(left_child, root, PAGE_SIZE);
	set_node_root(left_child, false);

	/*
	 * The root node now is a internal node
	*/
	initialize_internal_node(root);
	set_node_root(root, true);
	*internal_node_num_keys(root) = 1;
	*internal_node_child(root, 0) = left_child_page_num;
	uint32_t left_child_max_key = get_node_max_key(left_child);
	*internal_node_key(root, 0) = left_child_max_key;
	*internal_node_right_child(root) = right_child_page_num;
	*node_parent(left_child) = table->root_page_num;
	*node_parent(right_child) = table->root_page_num;
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
	uint32_t num_keys = *internal_node_num_keys(node);

  // binary search
	uint32_t start = 0;
	uint32_t end = num_keys;
	
	while(start != end) {
		uint32_t middle = (start + end) / 2;
		uint32_t key_to_right = *internal_node_key(node, middle);
		if (key_to_right >= middle) {
			end = middle;
		} else {
			start = middle + 1;
		}
	}

	return start;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
	uint32_t old_child_index = internal_node_find_child(node, old_key);
	*internal_node_key(node, old_child_index) = new_key;
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t new_page_num) {
	// Add a new child/key pair to parent that corresponds to child
	void* parent = get_page(table->pager, parent_page_num);
	void* child = get_page(table->pager, new_page_num);

	// child max key to insert in the parent
	uint32_t child_max_key = get_node_max_key(child);
	// position to insert the new child in the parent
	uint32_t child_max_num = internal_node_find_child(parent, child_max_key);

	uint32_t original_num_keys = *internal_node_num_keys(parent);
	*internal_node_num_keys(parent) = original_num_keys + 1;

	if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
		printf("Need to implement splitting internal node\n");
		exit(EXIT_FAILURE);
	}

	uint32_t right_child_num = *internal_node_right_child(parent);
	void* right_child = get_page(table->pager, right_child_num);
	
	if (child_max_key > get_node_max_key(right_child)) {
		// TODO: maybe this is redundant
		*internal_node_child(parent, original_num_keys) = right_child_num;
		*internal_node_key(parent, original_num_keys) = get_node_max_key(right_child);
		*internal_node_right_child(parent) = new_page_num;
	} else {
		for (uint32_t i = original_num_keys; i > child_max_num; i--) {
			void* destination = internal_node_cell(parent, i);
			void* source = internal_node_cell(parent, i - 1);
			memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
		}

		*internal_node_child(parent, child_max_num) = new_page_num;
		*internal_node_key(parent, child_max_num) = child_max_key;
	}
}

/*
 * Create a new node and move half the cells over.
 * Nem node will inserted in one of the two nodes.
*/
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
	void* old_node = get_page(cursor->table->pager, cursor->page_num); // root
	uint32_t old_max = get_node_max_key(old_node); // 5
	uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
	void* new_node = get_page(cursor->table->pager, new_page_num);
	initialize_leaf_node(new_node); // new 5 node
	*node_parent(new_node) = *node_parent(old_node); // receive the root as parent
  *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
  *leaf_node_next_leaf(old_node) = new_page_num;

	/*
	 * all key plus new key should be divided between left (old) and right (new) nodes
	*/
	for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
		void* destination_node;
		if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
			destination_node = new_node;
		} else {
			destination_node = old_node;
		}

		uint32_t node_indexed = i % LEAF_NODE_LEFT_SPLIT_COUNT;
		void* destination = leaf_node_cell(destination_node, node_indexed);

		if (i == cursor->cell_num) {
			serialize_row(value, leaf_node_value(destination_node, node_indexed));
			*leaf_node_key(destination_node, node_indexed) = key;
		} else if (i > cursor->cell_num) {
			memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
		} else {
			memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
		}
	}

	/* Update header cell count */
	*(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
	*(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
	
	if (is_node_root(old_node)) {
		return create_new_root(cursor->table, new_page_num);
	} else {
		uint32_t parent_page_num = *node_parent(old_node);
		void* parent = get_page(cursor->table->pager, parent_page_num);

		// update parent node with max key of old left leaf node
		uint32_t new_max = get_node_max_key(old_node);
		update_internal_node_key(parent, old_max, new_max); // 5 -> 3

		internal_node_insert(cursor->table, parent_page_num, new_page_num);
		return;
	}
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
  void* node = get_page(cursor->table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(node);

  if (num_cells >= LEAF_NODE_MAX_CELLS) {
    leaf_node_split_and_insert(cursor, key, value);
    return;
  }

	for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
		memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
	}

  *(leaf_node_num_cells(node)) += 1;
  *(leaf_node_key(node, cursor->cell_num)) = key;
  serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
	void* node = get_page(table->pager, page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);
	
	Cursor* cursor = malloc(sizeof(Cursor));
	cursor->table = table;
	cursor->page_num = page_num;

	uint32_t start = 0;
	uint32_t end = num_cells;

	while (start != end) {
		uint32_t middle = (start + end) / 2;
		uint32_t key_at_index = *leaf_node_key(node, middle);

		if (key == key_at_index) {
			cursor->cell_num = middle;
			return cursor;
		}

		if (key > key_at_index) {
			start = middle + 1;
		} else {
			end = middle;
		}
	}

	cursor->cell_num = start;
	return cursor;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
	void* node = get_page(table->pager, page_num);

	uint32_t child_index = internal_node_find_child(node, key);
	uint32_t child_num = *internal_node_child(node, child_index);
	
	void* child = get_page(table->pager, child_num);
	switch (get_node_type(child)) {
		case NODE_LEAF:
			return leaf_node_find(table, child_num, key);
		case NODE_INTERNAL:
			return internal_node_find(table, child_num, key);
	}
}

/* 
 * Return the position of the give key
 * If the key is not present, return the position where it should be inserted
*/
Cursor* table_find_by_key(Table* table, uint32_t key) {
	uint32_t root_page_num = table->root_page_num;
	void* root_node = get_page(table->pager, root_page_num);

	if (get_node_type(root_node) == NODE_LEAF) {
		return leaf_node_find(table, root_page_num, key);
	}

	return internal_node_find(table, root_page_num, key);		
} 

void* cursor_value(Cursor* cursor) {
	void* page = get_page(cursor->table->pager, cursor->page_num);
	return leaf_node_value(page, cursor->cell_num);
}

void print_row(Row r) {
	printf("(%d, %s, %s)\n", r.id, r.username, r.email);
}

Cursor* cursor_table_start(Table* table) {
	Cursor* cursor = table_find_by_key(table, 0);

	void* node = get_page(table->pager, cursor->page_num);
	uint32_t num_cells = *leaf_node_num_cells(node);
	cursor->end_of_table = (num_cells == 0);

	return cursor;
}

void advance_cursor(Cursor* cursor) {
	uint32_t page_num = cursor->page_num;
	void* node = get_page(cursor->table->pager, page_num);

	cursor->cell_num++;
	if (cursor->cell_num >= (*leaf_node_num_cells(node))) { 
		uint32_t next_page_num = *leaf_node_next_leaf(node); 
		if (next_page_num == 0) {
			// rightmost leaf
			cursor->end_of_table = true;
		} else {
			cursor->page_num = next_page_num;
			cursor->cell_num = 0;
		}
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
		set_node_root(root_node, true);
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
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

	free(pager);
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

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
	void* node = get_page(pager, page_num);
	uint32_t num_keys, child;

	switch (get_node_type(node)) {
		case (NODE_LEAF):
			num_keys = *leaf_node_num_cells(node);
			indent(indentation_level);
			printf("- leaf (size %d)\n", num_keys);
			for (uint32_t i = 0; i < num_keys; i++) {
				indent(indentation_level + 1);
				printf("- %d\n", *leaf_node_key(node, i));
			}
			break;
		case (NODE_INTERNAL):
			num_keys = *internal_node_num_keys(node);
			indent(indentation_level);
			printf("- internal (size %d)\n", num_keys);
			for (uint32_t i = 0; i < num_keys; i++) {
				child = *internal_node_child(node, i);
				print_tree(pager, child, indentation_level + 1);

				indent(indentation_level + 1);
				printf("- key %d\n", *internal_node_key(node, i));
			}
			child = *internal_node_right_child(node);
			print_tree(pager, child, indentation_level + 1);
			break;	
	}
}

MetaCommandResult do_meta_command(InputBuffer *ib, Table *table) {
	if (strcmp(ib->buffer, ".exit") == 0) {
		close_input_buffer(ib);
		close_db(table);
		exit(EXIT_SUCCESS);
	} else if (strcmp(ib->buffer, ".constants") == 0) {
		printf("Constants ->\n");
		print_constants();
		return META_COMMAND_SUCCESS;
	} else if (strcmp(ib->buffer, ".btree") == 0) {
		printf("Btree ->\n");
		print_tree(table->pager, 0, 0);
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
  uint32_t num_cells = *leaf_node_num_cells(node);

  Row* r = &(st->row_to_insert);
  uint32_t key_to_insert = r->id;
  Cursor* cursor = table_find_by_key(table, key_to_insert);

  if (cursor->cell_num < num_cells) {
    uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
    if (key_at_index == key_to_insert) {
      return EXECUTE_DUPLICATED_KEY;
    }
  }

  leaf_node_insert(cursor, r->id, r);

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *st, Table *table) {
	Row r;
	Cursor* cursor = cursor_table_start(table);

	while(!cursor->end_of_table) {
		deserialize_row(cursor_value(cursor), &r);
		print_row(r);
		advance_cursor(cursor);
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
			case (EXECUTE_DUPLICATED_KEY):
				printf("Error: duplicate key\n");
				break;
		}
	}
}

/********************************************************************
db.h - This file contains all the structures, defines, and function
prototype for the db.exe program.
*********************************************************************/

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL		16
#define MAX_TOK_LEN		32
#define KEYWORD_OFFSET	10
#define STRING_BREAK	" (),<>="
#define NUMBER_BREAK	" ),"
#define MAX_ROW_SIZE    528
#define MAX_RECORDS     500

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN + 4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
Minimum of 1 column in a table - therefore minimum size of
1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN + 4];
	int				num_columns;
	int				cd_offset;
	int       tpd_flags;
} tpd_entry;

typedef struct table_file_header_def
{
	int                    file_size;                                   // 4 bytes
	int                    record_size;                              // 4 bytes
	int                    num_records;                            // 4 bytes
	int                    record_offset;                           // 4 bytes
	int                    file_header_flag;                       // 4 bytes
	tpd_entry              *tpd_ptr;                                   // 4 bytes
} table_file_header;

typedef struct row_def
{

}row;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
table is defined the tpd_list is 48 bytes.  When there is
at least 1 table, then the tpd_entry (36 bytes) will be
overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry	tpd_start;
}tpd_list;


typedef struct conditions_def
{
	char *col_names[MAX_NUM_COL];
	char *values[MAX_NUM_COL];
	int and_or[MAX_NUM_COL];
	int operators[MAX_NUM_COL];
} conditions;


/* This token_list definition is used for breaking the command
string into separate tokens in function get_tokens().  For
each token, a new token_list will be allocated and linked
together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

typedef struct insert_row_def
{

}insr;

typedef struct update_row_def
	{

	}upsr;

/* This enum defines the different classes of tokens for
semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 			// 3
	type_name,		// 4
	constant,		  // 5
	function_name,// 6
	terminator,		// 7
	error			    // 8

} token_class;

/* This enum defines the different values associated with
a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR = 11,		    // 11 
	K_CREATE = 12, 		// 12
	K_TABLE = 13,			// 13
	K_NOT = 14,				// 14
	K_NULL = 15,				// 15
	K_DROP = 16,				// 16
	K_LIST = 17,				// 17
	K_SCHEMA = 18,			// 18
	K_FOR = 19,        // 19
	K_TO = 20,		    // 20
	K_INSERT = 21,     // 21
	K_INTO = 22,       // 22
	K_VALUES = 23,     // 23
	K_DELETE = 24,     // 24
	K_FROM = 25,       // 25
	K_WHERE = 26,      // 26
	K_UPDATE = 27,     // 27
	K_SET = 28,        // 28
	K_SELECT = 29,     // 29
	K_ORDER = 30,      // 30
	K_BY = 31,         // 31
	K_DESC = 32,       // 32
	K_IS = 33,         // 33
	K_AND = 34,        // 34
	K_OR = 35,         // 35 - new keyword should be added below this line
	F_SUM = 36,        // 36
	F_AVG = 37,        // 37
	F_COUNT = 38,      // 38 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN = 71,		  // 71
	S_COMMA = 72,			      // 72
	S_STAR = 73,             // 73
	S_EQUAL = 74,            // 74
	S_LESS = 75,             // 75
	S_GREATER = 76,          // 76
	IDENT = 85,			    // 85
	INT_LITERAL = 90,	  // 90
	STRING_LITERAL = 91,     // 91
	EOC = 95,			      // 95
	INVALID = 99		    // 99
	
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 29

/* New keyword must be added in the same position/order as the enum
definition above, otherwise the lookup will be wrong */
char *keyword_table[] =
{
	"int", "char", "create", "table", "not", "null", "drop", "list", "schema",
	"for", "to", "insert", "into", "values", "delete", "from", "where",
	"update", "set", "select", "order", "by", "desc", "is", "and", "or","sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
	INVALID_STATEMENT = -199,	// -199
	CREATE_TABLE = 100,				// 100
	DROP_TABLE,								// 101
	LIST_TABLE,								// 102
	LIST_SCHEMA,							// 103
	INSERT,                   // 104
	DELETE,                   // 105
	UPDATE,                   // 106
	SELECT,                    // 107
	BACKUP,
	RESTORE,
	ROLLFORWARD

} semantic_statement;

/* This enum has a list of all the errors that should be detected
by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME = -398,				// -398
	TABLE_NOT_EXIST = -397,						// -397
	INVALID_TABLE_DEFINITION = -396,		// -396
	INVALID_COLUMN_NAME = -395,				// -395
	DUPLICATE_COLUMN_NAME = -394,			// -394
	COLUMN_NOT_EXIST = -393,						// -393
	MAX_COLUMN_EXCEEDED = -392,				// -392
	INVALID_TYPE_NAME = -391,					// -391
	INVALID_COLUMN_DEFINITION = -390,	// -390
	INVALID_COLUMN_LENGTH = -389,			// -389
	INVALID_REPORT_FILE_NAME = -388,		// -388
	/* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,			// -299
	DBFILE_CORRUPTION = -298,					// -298
	MEMORY_ERROR = -297,							  // -297
	NULL_ASSIGNMENT = -296,
	INVALID_VALUE_LENGTH = -295,
	TYPE_MISMATCH = -294,
	INVALID_NUMBER_PARAMS = -293,
	MAX_NUM_ROWS_REACHED = -292,
	MAX_NUM_ROWS = -291,
	INVALID_INSERT_STATEMENT_DEF = -290,
	INVALID_UPDATE_STATEMENT_DEf = -287,
	INVALID_SELECT_STATEMENT_DEf = -256,
	INVALID_SELECT_STATEMENT_DEF = -234,
	COLUMN_LEN_MISMATCH = -289,
	COLUMN_NOT_FOUND = -276,
	INVALID_ORDER_BY_COLUMN = -443,
	RIGHT_PARENTHESIS_MISSING = -288,
	INVALID_DELETE_STATEMENT_DEF = -266,
	INVALID_RELATIONAL_OPERATOR = -255,
	INVALID_BACKUP_STATEMENT = 456,
	INVALID_IMAGE_FILE_NAME = 441,
	COLUMN_NOT_NULL = 432,

} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list, char *query);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int create_file(char *tname, int record_size);
int read_file(char *tname);
int sem_insert(token_list *t_list);
int update_file(table_file_header *table, char *fname);
int sem_select(token_list *t_list);
int sem_select(token_list *t_list);
int sem_update(token_list *t_list);
int sem_delete(token_list *t_list);
int add_entry_to_log(char *query);
int sem_backup(token_list *t_list);
int sem_restore(token_list *t_list);

/*
Keep a global list of tpd - in real life, this will be stored
in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);

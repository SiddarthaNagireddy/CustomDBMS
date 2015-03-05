/************************************************************
Project#1:	CLP & DDL
************************************************************/

#include "db.h"
#include <stdio.h>
#include <conio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <Windows.h>

# if defined(_MSC_VER)
# ifndef _CRT_SECURE_NO_DEPRECATE
# define _CRT_SECURE_NO_DEPRECATE (1)
# endif
# pragma warning(disable : 4996)
# endif

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;
	//printf("%d",argc);


	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
				tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			char *query = argv[1];
			rc = do_semantic(tok_list,query);
			//printf("%s",argv[1]);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr = tmp_tok_ptr;
		}
	}
	//getch();
	return rc;
}

/*************************************************************
This is a lexical analyzer for simple SQL statements
*************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc = 0, i, j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do
			{
				temp_string[i++] = *cur++;
			} while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank, (, ), or a comma, then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET + j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET + j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET + j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do
			{
				temp_string[i++] = *cur++;
			} while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				is not a blank or a ), then append this
				character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
			|| (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(': t_value = S_LEFT_PAREN; break;
			case ')': t_value = S_RIGHT_PAREN; break;
			case ',': t_value = S_COMMA; break;
			case '*': t_value = S_STAR; break;
			case '=': t_value = S_EQUAL; break;
			case '<': t_value = S_LESS; break;
			case '>': t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do
			{
				temp_string[i++] = *cur++;
			} while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list, char *query)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
		((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
		((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_SELECT)
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if (cur->tok_value == K_UPDATE)
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
		//printf("%s",cur->tok_string);
	}
	else if (cur->tok_value == K_DELETE)
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if( stricmp(cur->tok_string,"backup")==NULL)
		{
		printf("Backup statement\n");
		cur_cmd = BACKUP;
		cur = cur->next;
		}
	else if( stricmp(cur->tok_string,"restore")==NULL)
		{
		printf("Backup statement\n");
		cur_cmd = RESTORE;
		cur = cur->next;
		}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch (cur_cmd)
		{
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert(cur);
			break;
		case SELECT:
			rc = sem_select(cur);
			break;
		case UPDATE:
			rc = sem_update(cur);
			break;
		case DELETE:
			rc = sem_delete(cur);
			break;
		case BACKUP:
			rc = sem_backup(cur);
			break;
		case RESTORE:
			rc = sem_restore(cur);
			break;
		default:
			; /* no action */
		}
	}

	if(!rc && cur_cmd != SELECT && cur_cmd != BACKUP && cur_cmd!=RESTORE)
		{
			add_entry_to_log(query);
		}

	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	int record_size = 0;
	char tname[MAX_IDENT_LEN + 4]; // to store tab name and send to create tab file.
	FILE *fc = NULL;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	record_size = 0;
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Max Number of columns * Size */
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for (i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string) == 0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);
										record_size = record_size + sizeof(int)+1;

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}
										else if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value == K_NULL))
										{
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of S_INT processing
								else
								{
									// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											record_size += 1 + sizeof(char)* col_entry[cur_id].col_len;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value == K_NULL))
													{
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry)+
						sizeof(cd_entry)*	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							(void*)&tab_entry,
							sizeof(tpd_entry));

						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
							(void*)col_entry,
							sizeof(cd_entry)* tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);
						if (!rc)
						{
							rc = create_file(new_entry->table_name, record_size);
							printf("\n A Tab file has been created for table name: %s \n", new_entry->table_name);
						}
						//read_file(new_entry->table_name);
						free(new_entry);
					}
				}
			}
		}
	}
	return rc;
}

int create_file(char *tname, int record_size)
{
	table_file_header table;
	char name[MAX_IDENT_LEN + 4];
	FILE *fc = NULL;
	int rc = 0, check=0;
	memset(name, '\0', MAX_IDENT_LEN + 4);
	strcat(name, tname);
	strcat(name, ".tab");
	if ((fc = fopen(name, "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	memset(&table, '\0', sizeof(table_file_header));
	table.num_records = 0;
	table.record_size = record_size;
	check = table.record_size % 4;
	if (check != 0)
	{
		table.record_size += 4 - check;
	}
	//printf("%d%d%d", table.record_size, table.file_size, table.record_offset);
	table.file_size = table.record_size;
	table.file_header_flag = 1;
	table.record_offset = sizeof(table_file_header);
	fwrite(&table, table.record_offset, 1, fc);
	fclose(fc);
	return rc;
}

int read_file(char *tname)
{
	char name[MAX_IDENT_LEN + 4];
	FILE *fr = NULL;
	table_file_header table;
	memset(name, '\0', MAX_IDENT_LEN + 4);
	strcat(name, tname);
	strcat(name, ".tab");
	fr = fopen(name, "rbc");
	fread(&table, sizeof(table_file_header), 1, fr);
	printf("\n%d\n%d\n%d\n%d\n%d\n%d\n", table);
	return 1;
}

int sem_insert(token_list *t_list)
{
	int rc = 0,i, len1,len2;
	token_list *cur;
	token_list *Val=NULL, *Sval=NULL;
	cur = t_list;
	tpd_entry *tab_entry=NULL;
	cd_entry *col_entry = NULL, *col_cur = NULL;
	int valno = 0, os=0, len, value=0;
	table_file_header *table=NULL, *ntable=NULL;
	struct _stat f_st;
	insr *ri;
	FILE *fc = NULL;
	char /*tname[MAX_IDENT_LEN + 1],*/ fname[MAX_IDENT_LEN + 1];
	memset(fname, '\0', MAX_IDENT_LEN + 1);
	char *data[MAX_IDENT_LEN + 1];
	/*memset(tname, '\0', MAX_IDENT_LEN + 1);*/

	if ((cur->tok_class != keyword) && (cur->tok_class != identifier)
		&& (cur->tok_class != type_name))
	{
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
		printf("No Table Name");
	}
	else
	{
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			printf("\n Mentioned Table doesnot exists, Please create one before inserting \n");
		}
		else
		{
			strcat(fname, cur->tok_string);
			strcat(fname, ".tab");
			printf("\n The file %s has now been opened for writing \n", fname);
			cur = cur->next;
			if (cur->tok_value != K_VALUES)
			{
				rc = INVALID_INSERT_STATEMENT_DEF;
				cur->tok_value = INVALID;
				printf("Missing VALUES keyword in the statement! please correct it\n");
			}
			else
			{
				cur = cur->next;
				if (cur->tok_value != S_LEFT_PAREN)
				{
					rc = INVALID_INSERT_STATEMENT_DEF;
					cur->tok_value = INVALID;
					printf("Missing '(' Symbol in the statement\n");
					
				}			
				else
				{
					cur = cur->next; // Pointer is inside values clause
					//printf("Flow has entered into the values clause");
					Val = cur;
					Sval = cur;
					if (cur->tok_value == S_RIGHT_PAREN)
					{
						rc = INVALID_INSERT_STATEMENT_DEF;
						cur->tok_value = INVALID;
						printf("There are no values to insert\n");
					}
					else if (cur->tok_value == EOC)
					{
						rc = INVALID_INSERT_STATEMENT_DEF;
						cur->tok_value=INVALID;
						printf("Missing ')' Symbol in the statment");

					}
				}

				if (!rc)
				{
					//printf("\n Displaying first value in values clause %s", cur->tok_string);

					fc = fopen(fname, "rbc");
					_fstat(_fileno(fc), &f_st);
					table = (table_file_header*)calloc(1, f_st.st_size);
					memset(table, '\0', f_st.st_size);
					fread(table, sizeof(table_file_header), 1, fc);
					fclose(fc);
					printf(" Each record size %d\n", table->record_size);
					ri = (insr *)calloc(1, table->record_size);
					memset(ri, '\0', table->record_size);

					for (i = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
						i < tab_entry->num_columns;i++, col_entry++)
					{
						//checking null values
						//printf("\n running for %s\n", cur->tok_string);
						//printf("%d i value\n", i);
						if (col_entry->not_null != 0 && cur->tok_value == K_NULL)
							{
							printf("\n Placing Null where it should not be NULL");
							rc = NULL_ASSIGNMENT;
							cur->tok_value = INVALID;
							}
						if (!rc)
						{
							if (stricmp(cur->tok_string, ",") == 0)
							{
								//printf("COMMA Encountered\n");
								cur = cur->next;
								//printf("%s\n%d\n%d\n", cur->tok_string,cur->tok_value,col_entry->col_type);								
							}
							else
							{
								//printf("\n Donno What is going on");
							}
						}
						if (!rc)
							{
							//checking if it is int 
							 if ((col_entry->col_type == T_INT) && (cur->tok_value == INT_LITERAL))
								{
									len = sizeof(int);
									//printf("Adding: %s : to the inster string\n", cur->tok_string);
									//printf("The col length is %d and the str added len is %d\n",col_entry->col_len, strlen(cur->tok_string));
									memcpy(ri + os, &len, 1);
									os++;
									value = atoi(cur->tok_string);
									memcpy(ri + os, &value, sizeof(int));
									os += sizeof(int);
									cur = cur->next;
								}
							else if (col_entry->col_type==T_CHAR && cur->tok_value==STRING_LITERAL)
								{
									len1 = strlen(cur->tok_string);
									if (col_entry->col_len < len1)
									{
										rc = COLUMN_LEN_MISMATCH;
										printf("Value length exceeds given column length\n");
										cur->tok_value = INVALID;
									}
									else
									{
										//printf("Entered into string loop\n");
										memcpy(ri + os, &len1, 1);
										os++;
										memcpy(ri + os, cur->tok_string, col_entry->col_len);
										os += col_entry->col_len;
										cur = cur->next;
									}
								}
							else if (cur->tok_value == K_NULL && col_entry->not_null == 0)
							{
								len2 = 0;
								//printf("Column Length %d \n", len1);
								memcpy(ri + os, &len2, 1);
								os += 1 + col_entry->col_len;
								cur = cur->next;
							}
							else
							{
								printf("INVALID STATEMENT");
								rc = INVALID_STATEMENT;
								cur->tok_value = INVALID;
							}
						}
						//printf("\n exiting loop");
					}

					if (!rc)
					{
						if (cur->tok_value != S_RIGHT_PAREN || cur->next->tok_value != EOC)
						{
							rc = RIGHT_PARENTHESIS_MISSING;
							cur->tok_value = INVALID;
							printf("\n Missing Right Parenthesis ");
						//	printf("%d i value\n", i);
						//	printf("\nDropped out in s_right paren");
						}
						else
						{
							if ((fc = fopen(fname, "abc"))== NULL)
							{
								rc = FILE_OPEN_ERROR;
							}
							
							else {
								fwrite(ri, table->record_size, 1, fc);
								free(ri);
								fclose(fc);
								table->num_records++;
								rc = update_file(table,fname);
								//read_file(tab_entry->table_name);
							}
						}
					}

				}

			}

		}
	}
	if(!rc)
		{
			printf("\n Execution Successful \n");
			printf(" A new row has been inserted into the table \n");
		}
	return rc;
}

int update_file(table_file_header *tab, char *fname)
{
	int rc = 0;
	int Nsize = 0;
	FILE *fd = NULL;

	if ((fd = fopen(fname, "r+bc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		Nsize = tab->file_size + tab->record_size;
		tab->file_size = Nsize;
		fwrite(tab, sizeof(table_file_header), 1, fd);
		//fflush(fc);
		fclose(fd);
	}
	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0,relationop = 0,A_Value,W_ValueInt;
	char *A_ValueStr,*W_ValueStr;
	row *ri,*fi; char r_str[MAX_TOK_LEN+1000];
	int r_int,j;
	int result = 0;
	FILE *fd = NULL,*fc=NULL;
	table_file_header *table;
	cd_entry *col_entry = NULL;
	token_list *cur = 0;
	struct _stat f_st;
	char fname[MAX_IDENT_LEN + 1],fname1[MAX_IDENT_LEN + 1];
	tpd_entry *tab_entry=NULL;
	cur = t_list;
	bool whereflag = false,foundwherecolumn=false,ListTrue[MAX_RECORDS];

	if(cur->tok_value!=K_FROM)
		{
			rc = INVALID_STATEMENT;
			printf("Missing From Keyword");
			cur->tok_value = INVALID;
		}
	else
		{
			cur=cur->next;
			strcpy(fname, cur->tok_string);
			strcat(fname, ".tab");
			strcpy(fname1, cur->tok_string);
			strcat(fname1, "1.tab");
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
					printf("No table exists\n");
				}
			cur=cur->next;
			if(cur->tok_value == K_WHERE)
				{
					whereflag = true;
				}
			else if( cur -> tok_value == EOC && !rc )
				{
					//printf("\n Entered the printing section %s\n", fname);
					fc = fopen(fname, "r+tc");
					_fstat(_fileno(fc), &f_st);
					table = (table_file_header*)calloc(1, f_st.st_size);
					memset(table, '\0', f_st.st_size);
					fread(table, sizeof(table_file_header), 1, fc);
					//fclose(fc);
					if(table->num_records!=0)
						{
							printf("Table consists of %d records, Deleted all of them!", table->num_records);
						}
					else
						{
							printf("Table consists of 0 rows, Nothing to delete!");
						}
					if ((fd = fopen(fname, "w+bc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							table->file_size = table->record_size;
							table->num_records = 0;
							fwrite(table, sizeof(table_file_header), 1, fd);
							fclose(fd);
							fclose(fc);
						}
				}
			else
				{
					rc = INVALID_DELETE_STATEMENT_DEF;
					printf("Invalid Delete Statement syntax please correct it\n");
					cur->tok_value = INVALID;
				}

			if(whereflag && !rc)
				{
					cur = cur -> next;
					if( cur->tok_value == EOC)
						{
							rc = INVALID_DELETE_STATEMENT_DEF;
							printf("Invalid Syntax after WHERE clause\n");
							cur->tok_value = INVALID;
						}	
					if(!rc)
						{
							//Check if the column after where clause exists
							int colid2 = 0;
							int i;
							col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
								{
								if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
									{
										foundwherecolumn = true;
										printf("Deleting based on the column %s \n", col_entry->col_name);
										//colcollect = col_entry->col_name;
										colid2 = col_entry->col_id;
									}
								}

							//Now lets check for relational operator
							cur = cur -> next;
							if(cur->tok_value == EOC)
								{
									rc = INVALID_DELETE_STATEMENT_DEF;
									printf("INVALID STATEMENT\n");
									cur->tok_value = INVALID;
								}
							else if(cur->tok_value == S_GREATER)
								{
									relationop = S_GREATER;
								}
							else if(cur->tok_value == S_LESS)
								{
									relationop = S_LESS;
								}
							else if(cur->tok_value == S_EQUAL)
								{
									relationop = S_EQUAL;
								}
							else
								{
									rc = INVALID_DELETE_STATEMENT_DEF;
									printf("INVALID RELATIONAL OPERATOR\n");
									cur->tok_value = INVALID;
								}
							
							// Now lets store the value based on which we will have to delete the records
							cur = cur -> next;
							if(cur->tok_value == EOC)
								{
									rc = INVALID_DELETE_STATEMENT_DEF;
									printf("Invalid Delete Statement syntax please correct it\n");
									cur->tok_value = INVALID;
								}
							else
								{
									if(cur->tok_value == INT_LITERAL)
										{
										if(cur->tok_value!=15)
											{
											A_Value = atoi(cur->tok_string); // Store Integer
											}
										else
											{
											A_Value = 0;
											A_ValueStr = "";
											}
										}
									else
										{
										if(cur->tok_value!=15)
											{
												A_ValueStr = cur->tok_string; // Store Character
											}
										else
											{
												A_Value = 0;
												A_ValueStr = "";
											}
										}
								}
							
							//check if the statement ends 
							cur = cur -> next;
							if(cur->tok_value !=EOC)
								{
									rc = INVALID_DELETE_STATEMENT_DEF;
									printf("Invalid Delete Statement syntax please correct it\n");
									cur->tok_value = INVALID;
								}
							else
								{
									// Lets start with deleting the records. First lets identify the records to be deleted based on condition
									if(!rc) {
										fc = fopen(fname, "r+tc");
										fd = fopen(fname1, "wbc");
										_fstat(_fileno(fc), &f_st);
										table = (table_file_header*)calloc(1, f_st.st_size);
										memset(table, '\0', f_st.st_size);
										fread(table, sizeof(table_file_header), 1, fc);
										col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										int num = table->num_records;
										int nr=0;
										int r = 0;
										int os=0;
										int recosize1 = table->record_size;
										int colid =0,collen=0,colmat=0,value=0;
										//Here I will keep in mind the record number for which I will have to update
										int offset1 = 24;
										bool offsetflag1 = false;
										while (nr<num)
										{
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(colid2==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop)
																{
																	case S_EQUAL:
																			if(W_ValueInt == A_Value)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																			if(W_ValueInt > A_Value)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																			if(W_ValueInt < A_Value)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_ValueStr = (char *)calloc(1,col_entry->col_len);
																memset(W_ValueStr, '\0', col_entry->col_len);
																fread(W_ValueStr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(stricmp(W_ValueStr,A_ValueStr)==NULL)
																	{
																		ListTrue[nr] = true;
																	}
																else
																	{
																		ListTrue[nr] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
										}
										fseek(fc,0,SEEK_SET);
										int l,f=0,record_num;
										row *si[MAX_NUM_COL];
										nr=0;
										//Now I will start deleting records based on ListTrue Boolean array		
										/*nr=num;
										record_num = num;
										ri = (row *)calloc(1,table->file_size);
										memset(ri, '\0', table->file_size);		
										fread(ri,table->file_size,1,fc); 
										r = table->record_size;
										f = table ->file_size;
										fwrite(ri,table->file_size,1,fd);*/
										fi = (row *)calloc(1,24);
										memset(fi,'\0',24);
										fread(fi,24,1,fc);
										f = 24;
										nr=0;
										while(nr<num)
											{
											fseek(fc,f,SEEK_SET);
											si[nr] = (row *)calloc(1,table->record_size);
											memset(si[nr],'\0',table->record_size);
											fread(si[nr],table->record_size,1,fc);
											//printf("%s ",si[nr]);
											//fwrite(si[nr],table->record_size,1,fd);
											f = f+table->record_size;
											nr++;
											}
										fclose(fc);
										fc = fopen(fname, "wbc");
										fwrite(fi,24,1,fc);
										int x,y;
										for(x=0;x<num;x++)
											{
											if(ListTrue[x] == true)
												{
													for(y=num-1;y>=x;y--)
														{
														if( ListTrue[y] == false )
															{
																fseek(fc,0,SEEK_END);
																fwrite(si[y],table->record_size,1,fc);
																num--;
																break;
															}
														else
															{
																num--;
															}

														}
												}
											else
												{
													fseek(fc,0,SEEK_END);
													fwrite(si[x],table->record_size,1,fc);
												}
											}
											int num1=0;
											num1 = table->num_records;
											nr = 0;
											while(nr<num1)
												{
												if(ListTrue[nr])
													{
														result++;
													}
												nr++;
												}
											fseek(fc,0,SEEK_SET);										
											if(!rc)
												{
													int Nsize = 0;
													Nsize = table->record_size*num;
													table->file_size = Nsize;
													table->num_records = num;
													fwrite(table, sizeof(table_file_header), 1, fc);
													//fflush(fc);
														
												}
											
										}
								}
						}

				}
		}
		if(!rc)
			{
				fclose(fc);
				fclose(fd);
				printf("Delete Successfull, Total of %d records have been deleted\n",result);
			}
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0,len=0,i,k=0,s=0,j;
	int count=0;
	token_list *cur = 0,*cur2=0, *cur3=0, *aggcur=0, *aggcur1=0;
	row *ri; char *colcollect[MAX_IDENT_LEN];
	char *block = NULL;
	int col_collect_id[MAX_NUM_COL+10];
	char * blockptr = NULL;
	int sum_colid=-1, avg_colid=-1 , count_colid=-1;
	cur = t_list;
	aggcur = t_list;
	aggcur1 = t_list;
	FILE *fc = NULL;
	struct _stat f_st;
	table_file_header *table;
	char fname[MAX_IDENT_LEN + 1];
	tpd_entry *tab_entry=NULL;
	cd_entry *col_entry = NULL, *col_cur = NULL, *col_cur1 = NULL;
	bool col = false, sum = false, avg = false, count_flag=false, count_star = false; 
	bool star = true,sum_col=false,avg_col=false,count_col=false,agg=false,agg_where=false;
	//if(
	if(cur->tok_value == F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT) 
		{
			if(cur->tok_value == F_SUM)
				{
					sum = true;
					agg = true;
				}
			else if(cur->tok_value == F_AVG)
				{
					avg = true;
					agg = true;
				}
			else if(cur->tok_value == F_COUNT)
				{
					count_flag = true;
					agg = true;
				}
			cur = cur->next;
			if(cur->tok_value == S_LEFT_PAREN && sum)
				{
					aggcur1 = cur;
					do {
							cur = cur->next;	
						} while(cur->tok_value!=K_FROM);
					cur=cur->next;
					if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
							{
								rc = TABLE_NOT_EXIST;
								cur->tok_value = INVALID;
								printf("No table exists");
							}
							else
							{
								strcpy(fname, cur->tok_string);
								strcat(fname, ".tab");
								cur = cur->next;
								if(cur->tok_value==K_WHERE)
									{
									agg_where=true;
									}
							}
					cur = aggcur1;
					cur = cur->next;
					col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					for (j = 0;j< tab_entry->num_columns;j++,col_entry++)
						{
						if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
								{
								if(col_entry->col_type == T_INT)
									{
										sum_colid = col_entry->col_id;
										sum_col = true;
									}
								}
						}
					if(!sum_col)
						{
							rc = INVALID_COLUMN_NAME;
							cur->tok_value = INVALID;
							printf("Invalid column name is sum\n");
						}
				}
			else if(cur->tok_value == S_LEFT_PAREN && avg)
				{
					aggcur1 = cur;
					do {
							cur = cur->next;	
						} while(cur->tok_value!=K_FROM);
					cur=cur->next;
					if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
							{
								rc = TABLE_NOT_EXIST;
								cur->tok_value = INVALID;
								printf("No table exists");
							}
							else
							{
								strcpy(fname, cur->tok_string);
								strcat(fname, ".tab");
								cur = cur->next;
								if(cur->tok_value == K_WHERE)
									{
										agg_where = true;
									}
							}
					cur = aggcur1;
					cur = cur->next;
					col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					for (j = 0;j< tab_entry->num_columns;j++,col_entry++)
						{
						if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
								{
								if(col_entry->col_type == T_INT)
									{
										avg_colid = col_entry->col_id;
										avg_col = true;
									}
								}
						}
					if(!avg_col)
						{
							rc = INVALID_COLUMN_NAME;
							cur->tok_value = INVALID;
							printf("Invalid column name is sum\n");
				
						}
				}
			else if(cur->tok_value == S_LEFT_PAREN && count_flag)
				{
					aggcur1 = cur;
					do {
							cur = cur->next;	
						} while(cur->tok_value!=K_FROM);
					cur=cur->next;
					if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
							{
								rc = TABLE_NOT_EXIST;
								cur->tok_value = INVALID;
								printf("No table exists");
							}
							else
							{
								strcpy(fname, cur->tok_string);
								strcat(fname, ".tab");
								cur = cur->next;
								if(cur->tok_value==K_WHERE)
									{
									agg_where=true;
									}
							}
					cur = aggcur1;
					cur = cur->next;
					if(cur->tok_value == S_STAR)
						{
						count_star = true;
						}
					else
						{
					col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					for (j = 0;j< tab_entry->num_columns;j++,col_entry++)
						{
						if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
								{
								count_colid = col_entry->col_id;
								count_col = true;
								}
						}
					if(!count_col)
						{
							rc = INVALID_COLUMN_NAME;
							cur->tok_value = INVALID;
							printf("Invalid column name is sum\n");
						}
					}
			}
			cur = aggcur1;
			do {
					cur = cur->next;	
				} while(cur->tok_value!=K_FROM);
			cur= cur->next->next;
		}
	else if (cur->tok_value == S_STAR)
	{
		//printf("found STAR");
		cur = cur->next;
		if (cur->tok_value != K_FROM)
		{
			rc = INVALID_STATEMENT;
			printf("\n MISSING FROM KEYWORD \n");
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			strcpy(fname, cur->tok_string);
			strcat(fname, ".tab");
			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
					printf("No table exists");
				}
				else
				{
					cur = cur->next;
				}
			}
		  }
		}
	else if(cur->tok_value!=S_COMMA)
	{
		cur2 = cur;
		cur3 = cur;
		star=false;
		int i=0,j,c1=0,cc;
		//cur=cur->next;
		while(cur->tok_value!=K_FROM)
		{
			cur=cur->next;
			count++;
		}
		cur=cur->next;
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
					printf("No table exists");
			}
				else
			{
					strcpy(fname, cur->tok_string);
					strcat(fname, ".tab");
					cur = cur->next;
			}
		col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		while(k<count) 
			{
				//cur2 = cur3;
				c1=0;
				col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				for (j = 0;j< tab_entry->num_columns;j++,col_entry++)
				{
					if(cur2->tok_value!=S_COMMA)
					{
						cc=0;
						if(stricmp(cur2->tok_string,col_entry->col_name)==NULL)
						{
							//printf("Column name matched\n");
							col=true;
							colcollect[s]=cur2->tok_string;
							col_collect_id[s] = col_entry->col_id;
							//printf("Column Name %s",colcollect[s]);
							s++; 
							cur2 = cur2->next;
							j = tab_entry ->num_columns;//breaking loop
						}
						else
						{
							//printf("Increasing Counter \n");
							c1++;
						}
					}
					else
					{
						//printf("comma skipped\n");
						cur2=cur2->next;cc++;
						j=tab_entry->num_columns;//breaking loop
					}
				}
				if(c1==tab_entry->num_columns)
					{
						rc=INVALID_STATEMENT;
						printf("undefined Column Present");
						k=count;// breaking while loop
					}
				else if (cc==2)
					{
					rc=INVALID_STATEMENT;
					printf("Two Comma's back to back");
					k=count;// breaking while loop
					}
				//cur2 = cur2->next;
				k++;
			}
				if(col==false)
				{
					rc=INVALID_STATEMENT;
					cur->tok_value=INVALID;
					printf("Column not found or Invalid entry\n");
				}
		}
	else
	{
		rc=INVALID_STATEMENT;
		cur->tok_value=INVALID;
	}
	//Next Round;
	if (!rc && cur->tok_value == EOC && star==true && !agg)
		{
			printf("\n");
			char r_str[MAX_TOK_LEN+1000];
			int num = 0,nr=0,os,j,rl,it,r;
			int r_int;
			char s[512];char *sptr = NULL; //string pointer
			//printf("\n Entered the printing section %s\n", fname);
			fc = fopen(fname, "r+tc");
			_fstat(_fileno(fc), &f_st);
			table = (table_file_header*)calloc(1, f_st.st_size);
			memset(table, '\0', f_st.st_size);
			fread(table, sizeof(table_file_header), 1, fc);
			(" \n Displaying %d number of records in %s file \n", table->num_records, fname);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			col_cur=col_entry;
			num = table->num_records;
			if(num == 0)
				{
					printf("There are 0 rows in the table\n\n");
				}
			else
				{
					printf("There are %d number of records in the table \n\n",num);
				}
			//Reading and displaying the column names
			for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
			{			
			printf("|");
			int len=0;
			if(col_entry->col_len < strlen(col_entry->col_name))
				{
					len = strlen(col_entry->col_name);
				}
			else
				{
					len = col_entry -> col_len;
				}
			printf("%-*s",len,col_entry->col_name);
			len=len+10+col_entry->col_len;
			}		
			printf("|");
			printf("\n");
			//Now looking to display data
			while(nr<num)
			{
				ri = (row *)calloc(1,table->record_size);
				memset(ri, '\0', table->record_size);		
				fread(ri,table->record_size,1,fc); 
				r = table->record_size;
				os = 0;nr++;
				printf("\n");
				for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
						;j< tab_entry->num_columns;j++,col_entry++)
							{
								if (col_entry->col_type == T_INT) 
								{
									os++; // To skip file length container (1 Byte)
									printf("|");
									//r_int = (char *) malloc(sizeof(char)*24);
									memset(&r_int, '\0', sizeof(int));
									memcpy(&r_int, ri + os, sizeof(int));
									//fseek(fc,24*sizeof(char),SEEK_SET);
									//fread(r_int,sizeof(char)*4,1,fc);
									//int r_int1 = atoi(r_int);
									int len=0;
									if(col_entry->col_len < strlen(col_entry->col_name))
										{
											len = strlen(col_entry->col_name);
										}
									else
										{
											len = col_entry -> col_len;
										}
									if(r_int==0)
										{
											char *sh = "-";
											printf("%*s",len,sh);
										}
									else
										{
											printf("%*d",len,r_int);
										}
									os += sizeof(int);
								}
								else
								{
									os++; // To skip file length container (1 Byte)
									memset(r_str, '\0', table->record_size);
									memcpy(r_str, ri + os, col_entry->col_len);
									printf("|");
									//fseek(fc,r+os,SEEK_SET);
									//fread(&r_str,sizeof(col_entry->col_len),1,fc);
									int len=0;
									if(col_entry->col_len < strlen(col_entry->col_name))
										{
											len = strlen(col_entry->col_name);
										}
									else
										{
											len = col_entry -> col_len;
										}
									printf("%-*s", len,r_str);
									os += col_entry->col_len;
								}
							}
							printf("|");
						}
			fclose(fc);
			//memset(r_str, '\0', table->record_size);
		}
		else if (!rc && cur->tok_value == K_WHERE && !agg_where && !agg)
		{
			//printf("Entered Where clause baby\n");
			int wherecolid2=-1,wherecolid1=-1,wherecolid1type=-1,wherecolid2type=-1;
			cur = cur -> next; bool checkcol = false,checkcol1=false;
			int col_id1,col_id2,A_Value,A_Value1,orderby_col,W_ValueInt; bool ListTrue[MAX_NUM_COL+10][2];
			char *A_ValueStr="DUM",*A_ValueStr1="DUM",*W_Valuestr="DUM";
			bool ANDFLAG=false, ORFLAG=false,ORDERFLAG=false,SORTFLAG=false,orderby_col_flag=false;
			bool orderby_sort_desc=false,orderby_sort_inc=false;
			int relationop,relationop1,temp,orderby_col_type=-1;
			printf("\n");
			char r_str[MAX_TOK_LEN+1000];
			int m,os,nr=0,num,j,r_int;
			//printf("Entered Column run code\n\n");
			fc = fopen(fname, "r+tc");
			_fstat(_fileno(fc), &f_st);
			table = (table_file_header*)calloc(1, f_st.st_size);
			memset(table, '\0', f_st.st_size);
			fread(table, sizeof(table_file_header), 1, fc);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			int v,d,b,c;
			//Checking for first column if it exists 
			if(col==true) {
			for ( b = 0; b < s; b++) {
					for ( c = b,  d = b; c < s; c++) {
						if (col_collect_id[c] < col_collect_id[d])
							d = c;
					}
					v = col_collect_id[b];
					col_collect_id[b] = col_collect_id[d];
					col_collect_id[d] = v;
				}
			}
			for(int r=0;r< tab_entry->num_columns; r++,col_entry++)
				{
				if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
					{
						checkcol = true;	
						wherecolid1 = col_entry -> col_id;
						wherecolid1type = col_entry -> col_type;
					}
				}
			int nr1 = table->num_records;
			while(nr1>=0)
				{
					ListTrue[nr1][0] = false;
					ListTrue[nr1][1] = false;
					nr1--;
				}
			if(!checkcol)
				{
					rc = COLUMN_NOT_EXIST;
					cur->tok_value = INVALID;
					printf("Column doesnot exist\n");
				}
			if(!rc)
				{
					cur = cur->next;
					if(cur->tok_value == EOC)
					{
						rc = INVALID_SELECT_STATEMENT_DEF;
						printf("INVALID STATEMENT\n");
						cur2->tok_value = INVALID;
					}
					else if(cur->tok_value == S_GREATER)
					{
						relationop = S_GREATER;
					}
					else if(cur->tok_value == S_LESS)
					{
						relationop = S_LESS;
					}
					else if(cur->tok_value == S_EQUAL)
					{
						relationop = S_EQUAL;
					}
					else
					{
						rc = INVALID_SELECT_STATEMENT_DEf;
						printf("INVALID RELATIONAL OPERATOR\n");
						cur2->tok_value = INVALID;
					}
					cur = cur  -> next;

					if(cur->tok_value != EOC)
						{
							if(cur->tok_value == INT_LITERAL)
								{
									A_Value = atoi(cur->tok_string);
								}
							else if(cur->tok_value == K_NULL)
								{
									A_Value = 0;
									A_ValueStr = "NULL";
								}
							else 
								{
									A_ValueStr = cur->tok_string;
								}
						}
					else
						{
							rc = INVALID_SELECT_STATEMENT_DEf;
							printf("INVALID RELATIONAL OPERATOR\n");
							cur2->tok_value = INVALID;
						}
					cur = cur -> next;
					if(cur -> tok_value != K_AND && cur -> tok_value != K_OR && cur -> tok_value != K_ORDER && cur->tok_value !=EOC)
						{
							rc = INVALID_SELECT_STATEMENT_DEF;
							printf("INVALID SYNTAX\n");
							cur->tok_value = INVALID;
						}
					else if ( cur->tok_value !=EOC)
						{
						if(cur->tok_value == K_ORDER)
							{
							 // To be implemented
								ORDERFLAG = true;
							}
						else if(cur -> tok_value == K_AND)
							{
								ANDFLAG = true;
							}
						else if(cur -> tok_value == K_OR)
							{
								ORFLAG= true;
							}
						
						if(ANDFLAG || ORFLAG)
								{
									cur = cur -> next; 
									col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									for(int i=0;i< tab_entry->num_columns; i++,col_entry++)
										{
										if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
											{
												checkcol1 = true;	
												wherecolid2 = col_entry -> col_id;
											}
										}
									if(!checkcol1)
										{
											rc = COLUMN_NOT_EXIST;
											cur->tok_value = INVALID;
											printf("Column doesnot exist\n");
										}
									
									if(!rc)
									{
										cur = cur->next;
										if(cur->tok_value == EOC)
										{
											rc = INVALID_SELECT_STATEMENT_DEF;
											printf("INVALID STATEMENT\n");
											cur2->tok_value = INVALID;
										}
										else if(cur->tok_value == S_GREATER)
										{
											relationop1 = S_GREATER;
										}
										else if(cur->tok_value == S_LESS)
										{
											relationop1 = S_LESS;
										}
										else if(cur->tok_value == S_EQUAL)
										{
											relationop1 = S_EQUAL;
										}
										else
										{
											rc = INVALID_SELECT_STATEMENT_DEf;
											printf("INVALID RELATIONAL OPERATOR\n");
											cur->tok_value = INVALID;
										}
										cur = cur  -> next;
										if(cur->tok_value != EOC)
											{
												if(cur->tok_value == INT_LITERAL)
													{
														A_Value1 = atoi(cur->tok_string);
													}
												else if(cur->tok_value == K_NULL)
													{
														A_Value1 = 0;
														A_ValueStr1 = "NULL";
													}
												else
													{
														A_ValueStr1 = cur->tok_string;
													}
											}
										else
											{
												rc = INVALID_SELECT_STATEMENT_DEf;
												printf("INVALID RELATIONAL OPERATOR\n");
												cur2->tok_value = INVALID;
											}

									}
									cur = cur->next;
								}
						
						if(ORDERFLAG || cur->tok_value == K_ORDER)
							{
								cur = cur -> next->next;
								col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								for(int i=0;i< tab_entry->num_columns; i++,col_entry++)
									{
									if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
										{
											orderby_col_flag = true;	
											orderby_col = col_entry -> col_id;
											orderby_col_type = col_entry -> col_type;
										}
									}
								if(!orderby_col_flag)
									{
									rc = INVALID_SELECT_STATEMENT_DEF;
									cur->tok_value = INVALID;
									printf("Incorrect Order by Column \n");
									}
								cur = cur -> next;
								if(cur->tok_value == K_DESC)
									{
										orderby_sort_desc=true;
										cur = cur -> next;
										if(cur->tok_value != EOC)
											{
												rc = INVALID_SELECT_STATEMENT_DEF;
												cur->tok_value = INVALID;
												printf("Incorrect Syntax \n");
											}	
									}
								else
									{
										orderby_sort_inc=true;
									}
							}
					}	
					else
						{

						}
					
					if(!rc)
							{
										
								//printf("Sentence Parsed Successfully. Lets Filter Data Now\n");
								nr = 0;
								//Here I will keep in mind the record number for which I will have to display
								int recosize1 = table -> record_size;
								int offset1 = 24;
								int offset2 = 24;
								num = table -> num_records;
								bool offsetflag1 = false;
								while (nr<num)
								{
									for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(wherecolid1==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop)
																{
																	case S_EQUAL:
																				if(W_ValueInt == A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else 
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																				if(W_ValueInt > A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																				if(W_ValueInt < A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_Valuestr = (char *)calloc(1,col_entry->col_len);
																memset(W_Valuestr, '\0', col_entry->col_len);
																fread(W_Valuestr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(strcmp(W_Valuestr, "") == NULL )
																	{
																		W_Valuestr = "NULL";
																	}	
																if(stricmp(W_Valuestr,A_ValueStr)==NULL)
																	{
																		ListTrue[nr][0] = true;
																	}
																else
																	{
																		ListTrue[nr][0] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
										}
										//check only if second column also exists 
										if(checkcol1)
											{
											nr=0;
											offsetflag1 = false;
											offset1 = 24;
											while (nr<num)
											{
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(wherecolid2==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop1)
																{
																	case S_EQUAL:
																				if(W_ValueInt == A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else 
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																				if(W_ValueInt > A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																				if(W_ValueInt < A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_Valuestr = (char *)calloc(1,col_entry->col_len);
																memset(W_Valuestr, '\0', col_entry->col_len);
																fread(W_Valuestr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(strcmp(W_Valuestr, "") == NULL )
																	{
																		W_Valuestr = "NULL";
																	}
																if(stricmp(W_Valuestr,A_ValueStr1)==NULL)
																	{
																		ListTrue[nr][1] = true;
																	}
																else
																	{
																		ListTrue[nr][1] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
											}
										}
									/*	else
											{
												nr=0;
												while (nr<num)
													{
													ListTrue[nr][1] = false; nr++;
													}
											}*/
										int r;v=0;
										//Here i will display the data the way i want. 
										if(!rc && !ORDERFLAG)
										{
										col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
										for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
										{		

											if(!col)
												{
													printf("|");
												}
											else if(col_collect_id[v] == col_entry->col_id)
												{
													printf("|");
												}
											int len=0;
											if(col_entry->col_len < strlen(col_entry->col_name))
												{
													len = strlen(col_entry->col_name);
												}
											else
												{
													len = col_entry -> col_len;
												}
											if(!col)
												{
													printf("%-*s",len,col_entry->col_name);
												}
											else if(col_collect_id[v] == col_entry->col_id)
												{
													printf("%-*s",len,col_entry->col_name);
													v++;
												}
											len=len+10+col_entry->col_len;
										}		
										printf("|");
										printf("\n");
										nr = 0;v=0;
										//Now looking to display data
										fseek(fc,24,SEEK_SET);
										while(nr<num)
										{
											ri = (row *)calloc(1,table->record_size);
											memset(ri, '\0', table->record_size);		
											fread(ri,table->record_size,1,fc); 
											r = table->record_size;
											os = 0;v=0;
											if(ANDFLAG)
											{
												if(ListTrue[nr][1] && ListTrue[nr][0])
													{
														printf("\n");
													}
											}
											else
												{
													if(ListTrue[nr][1] || ListTrue[nr][0])
													{
														printf("\n");
													}
												}
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
													;j< tab_entry->num_columns;j++,col_entry++)
														{
															if (col_entry->col_type == T_INT) 
															{
																os++; // To skip file length container (1 Byte)
																if(ANDFLAG)
																	{
																		if(ListTrue[nr][1] && ListTrue[nr][0] && !col)
																			{
																				printf("|");
																			}
																		else if((ListTrue[nr][1] && ListTrue[nr][0]) && col_collect_id[v] == col_entry->col_id)
																			{
																				printf("|");
																			}
																	}
																else
																	{
																		if((ListTrue[nr][1] || ListTrue[nr][0]) && !col)
																			{
																				printf("|");
																			}
																		else if((ListTrue[nr][1] || ListTrue[nr][0]) && col_collect_id[v] == col_entry->col_id)
																			{
																				printf("|");
																			}
																	}
																//r_int = (char *) malloc(sizeof(char)*24);
																memset(&r_int, '\0', sizeof(int));
																memcpy(&r_int, ri + os, sizeof(int));
																//fseek(fc,24*sizeof(char),SEEK_SET);
																//fread(r_int,sizeof(char)*4,1,fc);
																//int r_int1 = atoi(r_int);
																int len=0;
																if(col_entry->col_len < strlen(col_entry->col_name))
																	{
																		len = strlen(col_entry->col_name);
																	}
																else
																	{
																		len = col_entry -> col_len;
																	}
																//AND or OR?
																if(ANDFLAG)
																	{
																		if(ListTrue[nr][1] && ListTrue[nr][0] && !col)
																			{
																				if(r_int==0)
																					{
																						char *ch="-";
																						printf("%*s",len,ch);
																					}
																				else
																					{
																						printf("%*d",len,r_int);
																					}
																			}
																		else if((ListTrue[nr][1] && ListTrue[nr][0]) && col_collect_id[v] == col_entry->col_id)
																			{
																				if(r_int==0)
																					{
																						char *ch="-";
																						printf("%*s",len,ch);
																					}
																				else
																					{
																						printf("%*d",len,r_int);
																					}
																				v++;
																			}
																	}
																else
																	{
																		if((ListTrue[nr][1] || ListTrue[nr][0]) && !col)
																			{
																				if(r_int==0)
																					{
																						char *ch="-";
																						printf("%*s",len,ch);
																					}
																				else
																					{
																						printf("%*d",len,r_int);
																					}
																			}
																		else if((ListTrue[nr][1] || ListTrue[nr][0]) && col_collect_id[v] == col_entry->col_id)
																			{
																				if(r_int==0)
																					{
																						char *ch="-";
																						printf("%*s",len,ch);
																					}
																				else
																					{
																						printf("%*d",len,r_int);
																					}
																				v++;
																			} 
																	}
																os += sizeof(int);
															}
															else
															{
																os++; // To skip file length container (1 Byte)
																memset(r_str, '\0', table->record_size);
																memcpy(r_str, ri + os, col_entry->col_len);
																if(ANDFLAG)
																	{
																		if((ListTrue[nr][1] && ListTrue[nr][0]) && !col)
																			{
																				printf("|");
																			}
																		else if((ListTrue[nr][1] && ListTrue[nr][0]) && col_collect_id[v]==col_entry->col_id)
																			{
																				printf("|");
																			}
																	}
																else
																	{
																	if((ListTrue[nr][1] || ListTrue[nr][0]) && !col)
																			{
																				printf("|");
																			}
																	else if((ListTrue[nr][1] || ListTrue[nr][0]) && col_collect_id[v]==col_entry->col_id)
																			{
																				printf("|");
																			}
																	}
																//fseek(fc,r+os,SEEK_SET);
																//fread(&r_str,sizeof(col_entry->col_len),1,fc);
																int len=0;
																if(col_entry->col_len < strlen(col_entry->col_name))
																	{
																		len = strlen(col_entry->col_name);
																	}
																else
																	{
																		len = col_entry -> col_len;
																	}
																if(ANDFLAG)
																	{
																		if((ListTrue[nr][1] && ListTrue[nr][0]) && !col)
																			{
																		printf("%-*s", len,r_str);
																			}
																		else if((ListTrue[nr][1] && ListTrue[nr][0]) && col_collect_id[v]==col_entry->col_id)
																			{
																		printf("%-*s", len,r_str);
																		//printf("|");
																		v++;
																			}
																	}
																else
																	{
																		if((ListTrue[nr][1] || ListTrue[nr][0]) && !col)
																			{
																		printf("%-*s", len,r_str);
																			}
																		else if((ListTrue[nr][1] || ListTrue[nr][0]) && col_collect_id[v]==col_entry->col_id)
																			{
																		printf("%-*s", len,r_str);
																		//printf("|");
																		v++;
																			}
																	}
																os += col_entry->col_len;
															}
														}
													if(ANDFLAG)
														{
															if((ListTrue[nr][1] && ListTrue[nr][0]))
															{
															printf("|");
															}
														}
													else
														{
															if((ListTrue[nr][1] || ListTrue[nr][0]))
															{
															printf("|");
															}
														}
													nr++;
													}

										}

										if(!rc && ORDERFLAG)
										{
											bool found_order_by = false;
											col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
											for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
											{		
												if(orderby_col == col_entry->col_id)
													{
														found_order_by = true;
													}
											}	
											if(!found_order_by)
												{
													rc = INVALID_ORDER_BY_COLUMN;
													cur->tok_value = INVALID;
													printf(" Invalid column name in order by clause\n");
												}
											else
												{
												printf("|");
												printf("\n");
												nr = 0;v=0;
												//SAVING ALL DATA IN A 3 - Dimensional Array
												//fseek(fc,24,SEEK_SET);
												int f = 24; int kr=0;
											
												int si[MAX_NUM_COL+10][1];
												char *ki[MAX_NUM_COL+10][1];
											
												char *v_str;
												nr=0; int value_int; char *value_str;
												bool offsetflag = false;
												while (nr<num)
													{
														for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
															;j< tab_entry->num_columns;j++,col_entry++)
															{	
															if(orderby_col==col_entry->col_id)
																	{
																	if(offsetflag == false)
																		{
																			f = f + 1;
																		}
																	//printf ("coll id is %d\n",offset);
																	fseek(fc,f,SEEK_SET);
																	if(col_entry->col_type == T_INT)
																		{
																			//kr = (row *)calloc(1,col_entry->col_len);
																			//memset(kr, '\0', col_entry->col_len);
																			fread(&kr,sizeof(int),1,fc);
																			//fread(&W_ValueInt,sizeof(int),1,fc);
																			//printf("Value is %d \n",W_ValueInt);
																			f = f + table->record_size;
																			si[nr][0] = kr;
																		}
																	else
																		{
																			//printf("%s/n",value_str);
																			v_str = (char *)calloc(1,col_entry->col_len);
																			memset(v_str, '\0', col_entry->col_len);
																			fread(v_str,col_entry->col_len,1,fc);
																			//fread(&W_ValueStr,col_entry->col_len,1,fc);
																			//printf("Value is %d \n",W_ValueStr);
																			f = f + table->record_size;
																			ki[nr][0] = v_str;
																		}
																		offsetflag = true;
																	}
																else
																	{
																		if(offsetflag==false)
																			{
																				f = f + col_entry->col_len+1;
																			}
																	}
															} nr++;
													}

												int b,c,d,v;
												char *xy;
												for ( b = 0; b < num; b++) {
														for ( c = b,  d = b; c < num; c++) {

															if(orderby_col_type == T_INT)
																{
																if(!orderby_sort_desc)
																	{
																	if (si[c][0] < si[d][0])
																		d = c;
																	}
																else
																	{
																	if (si[c][0] > si[d][0])
																		d = c;
																	}
																}
															else
																{
																		
																		if(!orderby_sort_desc)
																		{
																		if(stricmp(ki[c][0],ki[d][0]) < 0)
																				d = c;
																		}
																	else
																		{
																		if(stricmp(ki[c][0],ki[d][0]) < 0)
																				d = c;
																		}
																}
														}
														if(orderby_col_type==T_INT)
															{
																v = si[b][0];
																si[b][0] = si[d][0];
																si[d][0] = v;
															}
														else
															{
																xy = ki[b][0];
																ki[b][0] = ki[d][0];
																ki[d][0] = xy;
															}
													}

												//printf("hello");
										}	
										
										}
									}
							
					}	
			
		}
				
		else if (col == true && !rc)
		{
			printf("\n");
			char r_str[MAX_TOK_LEN+1000];
			int m,os,nr=0,num,j,r_int;
			//printf("Entered Column run code\n\n");
			fc = fopen(fname, "r+tc");
			_fstat(_fileno(fc), &f_st);
			table = (table_file_header*)calloc(1, f_st.st_size);
			memset(table, '\0', f_st.st_size);
			fread(table, sizeof(table_file_header), 1, fc);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			if(table->num_records == 0)
				{
					printf("There are 0 rows in the table\n\n");
				}
			else
				{
					printf("There are %d number of records in the table \n\n",table->num_records);
				}
			for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
			{		
				int l =0;
				while(l<s){
					if(stricmp(colcollect[l],col_entry->col_name)==NULL)
						{
							printf("|");
							if(col_entry->col_len < strlen(col_entry->col_name))
							{
								len = strlen(col_entry->col_name);
							}
						else
							{
								len = col_entry -> col_len;
							}
							printf("%-*s",len,col_entry->col_name);
							len=len+10+col_entry->col_len;
						}l++;
					}
			}		
			printf("|");
			printf("\n");
			num = table->num_records;
			while(nr<num)
			{
				ri = (row *)calloc(1,table->record_size);
				memset(ri, '\0', table->record_size);		
				fread(ri,table->record_size,1,fc);
				nr++;os = 0;
				printf("\n");
				for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
						;j< tab_entry->num_columns;j++,col_entry++)
							{
								int l=0,flag=0;
								while(l<s){
									if(stricmp(colcollect[l],col_entry->col_name)==NULL)
									{
										if (col_entry->col_type == T_INT) 
										{
											os++; // To skip file length container (1 Byte)
											printf("|");
											memset(&r_int, '\0', sizeof(int));
											memcpy(&r_int, ri + os, sizeof(int));
											int len=0;
											if(col_entry->col_len < strlen(col_entry->col_name))
												{
													len = strlen(col_entry->col_name);
												}
											else
												{
													len = col_entry -> col_len;
												}
											if(r_int==0)
												{
													char *sh = "-";
													printf("%*s",len,sh);
												}
											else
												{
													printf("%*d",len,r_int);
												}
											os += sizeof(int);
										}
										else
										{
											os++; // To skip file length container (1 Byte)
											memset(r_str, '\0', table->record_size);
											memcpy(r_str, ri + os, col_entry->col_len);
											printf("|");
											int len=0;
											if(col_entry->col_len < strlen(col_entry->col_name))
												{
													len = strlen(col_entry->col_name);
												}
											else
												{
													len = col_entry -> col_len;
												}
											printf("%-*s",len,r_str);
											os += col_entry->col_len;
										}
									}
									else
										{
												flag++;
										}
									l++;
								}
								if (flag == s)
									{
										if (col_entry->col_type == T_INT) 
												{
													os++;
													os += sizeof(int);
												}
												else
												{
													os++;
													os += col_entry->col_len;
												}
									}

							}
							printf("|");
						}
			fclose(fc);
		}

		else if (agg && !rc && !agg_where)
			{
			printf("\n");
			char r_str[MAX_TOK_LEN+1000];
			int num = 0,nr=0,os,j,rl,it,r;
			int r_int;
			char s[512];char *sptr = NULL; //string pointer
			//printf("\n Entered the printing section %s\n", fname);
			fc = fopen(fname, "r+tc");
			_fstat(_fileno(fc), &f_st);
			table = (table_file_header*)calloc(1, f_st.st_size);
			memset(table, '\0', f_st.st_size);
			fread(table, sizeof(table_file_header), 1, fc);
			(" \n Displaying %d number of records in %s file \n", table->num_records, fname);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			col_cur=col_entry;
			int count_agg=0, sum=0;
			num = table->num_records;
			int gen_colid=-1;
			//Reading and displaying the column names
			if (count_flag)
				{
				if(count_col)
					{
					gen_colid = count_colid;
					}
				else
					{
					gen_colid = 0;
					}
				}
			else if(avg)
				{
				gen_colid = avg_colid;
				}
			else
				{
				gen_colid = sum_colid;
				}
				
			for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
			{			
			if(gen_colid == col_entry->col_id && !count_star)
				{
					printf("-----------------\n");
					printf("|");
					int len=15;
					printf("%-*s",len,col_entry->col_name);
					len=len+10+col_entry->col_len;
				}
			}	
			if(count_star)
			{
				printf("-----------------\n");
				printf("|");
				int len=15;
				char *ph = "Count(*)";
				printf("%-*s",len,ph);
			}
			printf("|\n");
			printf("-----------------\n");
			//Now looking to display data
			while(nr<num)
			{
				ri = (row *)calloc(1,table->record_size);
				memset(ri, '\0', table->record_size);		
				fread(ri,table->record_size,1,fc); 
				r = table->record_size;
				os = 0;nr++;
				//printf("\n");
				for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
						;j< tab_entry->num_columns;j++,col_entry++)
							{
								if (col_entry->col_type == T_INT) 
								{
									os++; // To skip file length container (1 Byte)
									//printf("|");
									//r_int = (char *) malloc(sizeof(char)*24);
									memset(&r_int, '\0', sizeof(int));
									memcpy(&r_int, ri + os, sizeof(int));
									//fseek(fc,24*sizeof(char),SEEK_SET);
									//fread(r_int,sizeof(char)*4,1,fc);
									//int r_int1 = atoi(r_int);
									if(gen_colid == col_entry->col_id && !count_star)
									{
										if(r_int==0)
											{
												char *sh = "-";
												int c = r_int;
												//printf("%*s",len,sh);
												sum = sum + c;
												if(!count_col)
													{
														count_agg++;
													}
											}
										else
											{
												//printf("%*d",len,r_int);
												int c = r_int;
												sum = sum + c;
												count_agg++;
											}
									}
									os += sizeof(int);
								}
								else
								{
									os++; // To skip file length container (1 Byte)
									memset(r_str, '\0', table->record_size);
									memcpy(r_str, ri + os, col_entry->col_len);
									//printf("|");
									//fseek(fc,r+os,SEEK_SET);
									//fread(&r_str,sizeof(col_entry->col_len),1,fc);
									if(gen_colid == col_entry->col_id && !count_star)
									{
										if(stricmp(r_str,"")==NULL)
											{
												if(!count_col)
													{
														count_agg++;
													}
											}
										else
											{
												count_agg++;
											}
									}
									//printf("%-*s", len,r_str);
									os += col_entry->col_len;
								}
							}
						//	printf("|");
						}
			if(count_star || count_col)
				{
					if(count_col)
						{
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,count_agg);
					printf("|\n");
					printf("-----------------");
						}
					else
						{
						count_agg = table->num_records;
						printf("-----------------\n");
						printf("|");
						printf("%*d",15,count_agg);
						printf("|\n");
						printf("-----------------");
						}
				}
			else if(avg)
				{
					count_agg = table->num_records;
					int avg1 =  sum / count_agg;
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,avg1);
					printf("|\n");
					printf("-----------------");
				}
			else
				{
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,sum);
					printf("|\n");
					printf("-----------------");
				}
				fclose(fc);
			}

		else if (!rc && cur->tok_value == K_WHERE && agg_where)
		{
			//printf("Entered aggregate Where clause baby\n");
			int wherecolid2=-1,wherecolid1=-1,wherecolid1type=-1,wherecolid2type=-1;
			cur = cur -> next; bool checkcol = false,checkcol1=false;
			int col_id1,col_id2,A_Value,A_Value1,orderby_col,W_ValueInt; bool ListTrue[MAX_NUM_COL+10][2];
			char *A_ValueStr="DUM",*A_ValueStr1="DUM",*W_Valuestr="DUM";
			bool ANDFLAG=false, ORFLAG=false,ORDERFLAG=false,SORTFLAG=false,orderby_col_flag=false;
			bool orderby_sort_desc=false,orderby_sort_inc=false;
			int relationop,relationop1,temp,orderby_col_type=-1;
			printf("\n"); int gen_colid = -1;
			int count_ag = 0,sum=0;
			char r_str[MAX_TOK_LEN+1000];
			int m,os,nr=0,num,j,r_int;
			//printf("Entered Column run code\n\n");
			fc = fopen(fname, "r+tc");
			_fstat(_fileno(fc), &f_st);
			table = (table_file_header*)calloc(1, f_st.st_size);
			memset(table, '\0', f_st.st_size);
			fread(table, sizeof(table_file_header), 1, fc);
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			int v,d,b,c;
			//setting all values
			if (count_flag)
				{
				if(count_col)
					{
					gen_colid = count_colid;
					}
				else
					{
					gen_colid = -1;
					}
				}
			else if(avg)
				{
				gen_colid = avg_colid;
				}
			else
				{
				gen_colid = sum_colid;
				}

				
			for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
			{			
				if(gen_colid == col_entry->col_id && !count_star)
					{
						printf("-----------------\n");
						printf("|");
						int len=15;
						printf("%-*s",len,col_entry->col_name);
						len=len+10+col_entry->col_len;
					}
			}	

			if(count_star)
			{
				printf("-----------------\n");
				printf("|");
				int len=15;
				char *ph = "Count(*)";
				printf("%-*s",len,ph);
			}
			printf("|");
			printf("\n");
			//Checking for first column if it exists 
			if(col==true) {
			for ( b = 0; b < s; b++) {
					for ( c = b,  d = b; c < s; c++) {
						if (col_collect_id[c] < col_collect_id[d])
							d = c;
					}
					v = col_collect_id[b];
					col_collect_id[b] = col_collect_id[d];
					col_collect_id[d] = v;
				}
			}
			col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			for(int i=0;i< tab_entry->num_columns; i++,col_entry++)
				{
				if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
					{
						checkcol = true;	
						wherecolid1 = col_entry -> col_id;
						wherecolid1type = col_entry -> col_type;
					}
				}
			int nr1 = table->num_records;
			while(nr1>=0)
				{
					ListTrue[nr1][0] = false;
					ListTrue[nr1][1] = false;
					nr1--;
				}
			if(!checkcol)
				{
					rc = COLUMN_NOT_EXIST;
					cur->tok_value = INVALID;
					printf("Column doesnot exist\n");
				}
			if(!rc)
				{
					cur = cur->next;
					if(cur->tok_value == EOC)
					{
						rc = INVALID_SELECT_STATEMENT_DEF;
						printf("INVALID STATEMENT\n");
						cur2->tok_value = INVALID;
					}
					else if(cur->tok_value == S_GREATER)
					{
						relationop = S_GREATER;
					}
					else if(cur->tok_value == S_LESS)
					{
						relationop = S_LESS;
					}
					else if(cur->tok_value == S_EQUAL)
					{
						relationop = S_EQUAL;
					}
					else
					{
						rc = INVALID_SELECT_STATEMENT_DEf;
						printf("INVALID RELATIONAL OPERATOR\n");
						cur2->tok_value = INVALID;
					}
					cur = cur  -> next;

					if(cur->tok_value != EOC)
						{
							if(cur->tok_value == INT_LITERAL)
								{
									A_Value = atoi(cur->tok_string);
								}
							else if(cur->tok_value == K_NULL)
								{
									A_Value = 0;
									A_ValueStr = "NULL";
								}
							else 
								{
									A_ValueStr = cur->tok_string;
								}
						}
					else
						{
							rc = INVALID_SELECT_STATEMENT_DEf;
							printf("INVALID RELATIONAL OPERATOR\n");
							cur2->tok_value = INVALID;
						}
					cur = cur -> next;
					if(cur -> tok_value != K_AND && cur -> tok_value != K_OR && cur -> tok_value != K_ORDER && cur->tok_value !=EOC)
						{
							rc = INVALID_SELECT_STATEMENT_DEF;
							printf("INVALID SYNTAX\n");
							cur->tok_value = INVALID;
						}
					else if ( cur->tok_value !=EOC)
						{
						if(cur->tok_value == K_ORDER)
							{
							 // To be implemented
								ORDERFLAG = true;
							}
						else if(cur -> tok_value == K_AND)
							{
								ANDFLAG = true;
							}
						else if(cur -> tok_value == K_OR)
							{
								ORFLAG= true;
							}
						
						if(ANDFLAG || ORFLAG)
								{
									cur = cur -> next; 
									col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									for(int i=0;i< tab_entry->num_columns; i++,col_entry++)
										{
										if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
											{
												checkcol1 = true;	
												wherecolid2 = col_entry -> col_id;
											}
										}
									if(!checkcol1)
										{
											rc = COLUMN_NOT_EXIST;
											cur->tok_value = INVALID;
											printf("Column doesnot exist\n");
										}
									
									if(!rc)
									{
										cur = cur->next;
										if(cur->tok_value == EOC)
										{
											rc = INVALID_SELECT_STATEMENT_DEF;
											printf("INVALID STATEMENT\n");
											cur2->tok_value = INVALID;
										}
										else if(cur->tok_value == S_GREATER)
										{
											relationop1 = S_GREATER;
										}
										else if(cur->tok_value == S_LESS)
										{
											relationop1 = S_LESS;
										}
										else if(cur->tok_value == S_EQUAL)
										{
											relationop1 = S_EQUAL;
										}
										else
										{
											rc = INVALID_SELECT_STATEMENT_DEf;
											printf("INVALID RELATIONAL OPERATOR\n");
											cur->tok_value = INVALID;
										}
										cur = cur  -> next;
										if(cur->tok_value != EOC)
											{
												if(cur->tok_value == INT_LITERAL)
													{
														A_Value1 = atoi(cur->tok_string);
													}
												else if(cur->tok_value == K_NULL)
													{
														A_Value1 = 0;
														A_ValueStr1 = "NULL";
													}
												else
													{
														A_ValueStr1 = cur->tok_string;
													}
											}
										else
											{
												rc = INVALID_SELECT_STATEMENT_DEf;
												printf("INVALID RELATIONAL OPERATOR\n");
												cur2->tok_value = INVALID;
											}

									}
									cur = cur->next;
								}
						/*
						if(ORDERFLAG || cur->tok_value == K_ORDER)
							{
								cur = cur -> next->next;
								col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								for(int i=0;i< tab_entry->num_columns; i++,col_entry++)
									{
									if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
										{
											orderby_col_flag = true;	
											orderby_col = col_entry -> col_id;
											orderby_col_type = col_entry -> col_type;
										}
									}
								if(!orderby_col_flag)
									{
									rc = INVALID_SELECT_STATEMENT_DEF;
									cur->tok_value = INVALID;
									printf("Incorrect Order by Column \n");
									}
								cur = cur -> next;
								if(cur->tok_value == K_DESC)
									{
										orderby_sort_desc=true;
										cur = cur -> next;
										if(cur->tok_value != EOC)
											{
												rc = INVALID_SELECT_STATEMENT_DEF;
												cur->tok_value = INVALID;
												printf("Incorrect Syntax \n");
											}	
									}
								else
									{
										orderby_sort_inc=true;
									}
							}
					    */
						}	
					else
						{

						}
					
					if(!rc)
							{
										
								//printf("Sentence Parsed Successfully. Lets Filter Data Now\n");
								nr = 0;
								//Here I will keep in mind the record number for which I will have to display
								int recosize1 = table -> record_size;
								int offset1 = 24;
								int offset2 = 24;
								num = table -> num_records;
								bool offsetflag1 = false;
								while (nr<num)
								{
									for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(wherecolid1==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop)
																{
																	case S_EQUAL:
																				if(W_ValueInt == A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else 
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																				if(W_ValueInt > A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																				if(W_ValueInt < A_Value)
																					{
																						ListTrue[nr][0] = true;
																					}
																				else
																					{
																						ListTrue[nr][0] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_Valuestr = (char *)calloc(1,col_entry->col_len);
																memset(W_Valuestr, '\0', col_entry->col_len);
																fread(W_Valuestr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(strcmp(W_Valuestr, "") == NULL )
																	{
																		W_Valuestr = "NULL";
																	}	
																if(stricmp(W_Valuestr,A_ValueStr)==NULL)
																	{
																		ListTrue[nr][0] = true;
																	}
																else
																	{
																		ListTrue[nr][0] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
										}
										//check only if second column also exists 
										if(checkcol1)
											{
											nr=0;
											offsetflag1 = false;
											offset1 = 24;
											while (nr<num)
											{
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(wherecolid2==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop1)
																{
																	case S_EQUAL:
																				if(W_ValueInt == A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else 
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																				if(W_ValueInt > A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																				if(W_ValueInt < A_Value1)
																					{
																						ListTrue[nr][1] = true;
																					}
																				else
																					{
																						ListTrue[nr][1] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_Valuestr = (char *)calloc(1,col_entry->col_len);
																memset(W_Valuestr, '\0', col_entry->col_len);
																fread(W_Valuestr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(strcmp(W_Valuestr, "") == NULL )
																	{
																		W_Valuestr = "NULL";
																	}
																if(stricmp(W_Valuestr,A_ValueStr1)==NULL)
																	{
																		ListTrue[nr][1] = true;
																	}
																else
																	{
																		ListTrue[nr][1] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
											}
										}
									/*	else
											{
												nr=0;
												while (nr<num)
													{
													ListTrue[nr][1] = false; nr++;
													}
											}*/
										int r;v=0;
										//Here i will display the data the way i want. 
										if(!rc && !ORDERFLAG)
										{
										nr = 0;v=0;
										//Now looking to display data
										fseek(fc,24,SEEK_SET);
										while(nr<num)
										{
											ri = (row *)calloc(1,table->record_size);
											memset(ri, '\0', table->record_size);		
											fread(ri,table->record_size,1,fc); 
											r = table->record_size;
											os = 0;v=0;
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
													;j< tab_entry->num_columns;j++,col_entry++)
														{
															if (col_entry->col_type == T_INT) 
															{
																os++; // To skip file length container (1 Byte)
																//r_int = (char *) malloc(sizeof(char)*24);
																memset(&r_int, '\0', sizeof(int));
																memcpy(&r_int, ri + os, sizeof(int));
																//fseek(fc,24*sizeof(char),SEEK_SET);
																//fread(r_int,sizeof(char)*4,1,fc);
																//int r_int1 = atoi(r_int);
																//AND or OR?
																if(ANDFLAG)
																	{
																	if((ListTrue[nr][1] && ListTrue[nr][0]) && gen_colid == col_entry->col_id)
																			{
																				if(r_int==0 && count_col)
																					{
																						//char *ch="-";
																						//printf("%*s",len,ch);
																					}
																				else
																					{
																						count_ag++;
																						sum = sum + r_int;
																						//printf("%*d",len,r_int);
																					}
																				v++;
																			}
																	else if((ListTrue[nr][1] && ListTrue[nr][0]) && count_star)
																		{
																			count_ag++;
																			//sum = sum + r_int;
																			j = tab_entry->num_columns;
																		}
																	}
																else
																	{
																	if((ListTrue[nr][1] || ListTrue[nr][0]) && gen_colid == col_entry->col_id)
																			{
																			if(r_int==0 && count_col)
																					{
																						//char *ch="-";
																						//printf("%*s",len,ch);
																					}
																				else
																					{
																						sum = sum + r_int;
																						count_ag++;
																						//printf("%*d",len,r_int);
																					}
																				v++;
																			} 
																		else if((ListTrue[nr][1] || ListTrue[nr][0]) && count_star)
																			{
																				count_ag++;
																				sum = sum + r_int;
																				j = tab_entry->num_columns;
																			}
																	}
																os += sizeof(int);
															}
															else
															{
																os++; // To skip file length container (1 Byte)
																memset(r_str, '\0', table->record_size);
																memcpy(r_str, ri + os, col_entry->col_len);
																//fseek(fc,r+os,SEEK_SET);
																//fread(&r_str,sizeof(col_entry->col_len),1,fc);
																if(ANDFLAG)
																	{
																	if((ListTrue[nr][1] && ListTrue[nr][0]) && gen_colid==col_entry->col_id)
																			{
																				//printf("%-*s", len,r_str);
																			count_ag++;
																		//printf("|");
																		v++;
																			}
																	else if((ListTrue[nr][1] && ListTrue[nr][0]) && count_star)
																		{
																			count_ag++;
																			j = tab_entry->num_columns;
																		}
																	}
																else
																	{
																		if((ListTrue[nr][1] || ListTrue[nr][0]) && gen_colid == col_entry->col_id)
																			{
																				//printf("%-*s", len,r_str);
																		//printf("|");
																				count_ag++;
																				v++;
																			}
																			else if((ListTrue[nr][1] || ListTrue[nr][0]) && count_star)
																			{
																				count_ag++;
																				j = tab_entry->num_columns;
																			}
																	}
																os += col_entry->col_len;
															}
														}
													nr++;
													}

										}

										
									}
							
					}	
				if(count_star || count_col)
				{
					if(count_col)
						{
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,count_ag);
					printf("|\n");
					printf("-----------------");
						}
					else
						{
						//count_ag = table->num_records;
						printf("-----------------\n");
						printf("|");
						printf("%*d",15,count_ag);
						printf("|\n");
						printf("-----------------");
						}
				}
				else if(avg)
				{
					int avg1=0;
					//count_ag = table->num_records;
					if(count_ag == 0)
						{
							avg1 = 0;
						}
					else
						{
						avg1 = sum / count_ag;
						}
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,avg1);
					printf("|\n");
					printf("-----------------");
				}
			else
				{
					printf("-----------------\n");
					printf("|");
					printf("%*d",15,sum);
					printf("|\n");
					printf("-----------------");
				}
		}

		else 
		{
			rc = INVALID_STATEMENT;
			printf("Invalid Statement");
			cur->tok_value = INVALID;
		}
	return rc;
}

int sem_update(token_list *t_list)
{
	int rc=0,i,num,nr,os,j,s=0,r_int;
	int result = 0;
	char r_str[MAX_TOK_LEN];
	FILE *fc;
	int value_int=0,r,currpos,newpos,A_Value,W_ValueInt;
	struct _stat f_st;
	token_list *cur=0,*cur2=0;
	cur = t_list;
	row *ri; 
	upsr *ui;
	char *colcollect, *value_str="NULL", *A_ValueStr, *W_ValueStr;
	bool foundflag = false,offsetflag = false,foundwhere = false,foundWhereflag=false;
	int relationop = 0;int valtype;
	table_file_header *table;
	char fname[MAX_IDENT_LEN+1];
	bool ListTrue[MAX_RECORDS+1];
	tpd_entry *tab_entry=NULL;
	cd_entry *col_entry=NULL, *col_cur=NULL;
	//printf("%s",cur->tok_string);
	if((tab_entry = get_tpd_from_list(cur->tok_string))==NULL)
		{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
				printf(" No such table exists! Please create one \n");
		}
	else
		{
			strcpy(fname, cur->tok_string);
			strcat(fname, ".tab");
			cur = cur -> next;
			printf("Opening file %s to update \n", fname);
			if(cur->tok_value != K_SET)
				{
					rc = INVALID_UPDATE_STATEMENT_DEf;
					cur->tok_value = INVALID;
					printf(" Missing keyword SET \n");
				}
			else
				{
					cur = cur->next;
					fc = fopen(fname, "r+bc");
					_fstat(_fileno(fc), &f_st);
					table = (table_file_header*)calloc(1, f_st.st_size);
					memset(table, '\0', f_st.st_size);
					fread(table, sizeof(table_file_header), 1, fc);
					col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
					for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
						{
						if(stricmp(cur->tok_string,col_entry->col_name)==NULL)
							{
								foundflag = true;
								printf("Updating %s column\n", col_entry->col_name);
								colcollect = col_entry->col_name;
							}
						}
					cur2 = cur;
					cur2 = cur2 -> next -> next -> next; //Checking for where
							if(cur2->tok_value == K_WHERE)
								{
									foundwhere = true;
									printf("Encountered Where in Update Statement\n");
								}
							if(foundflag && !foundwhere)
							{
									cur=cur->next;
									if(cur->tok_value == EOC)
										{
											rc = INVALID_UPDATE_STATEMENT_DEf;
											printf("INVALID STATEMENT\n");
											cur->tok_value = INVALID;
										}
									cur=cur->next;
									if(cur->tok_value == EOC)
										{
											rc = INVALID_UPDATE_STATEMENT_DEf;
											printf("INVALID STATEMENT\n");
											cur->tok_value=INVALID;
										}
									else
										{
										//printf("%s",cur->tok_string);
										valtype = cur->tok_value;
										if(cur->tok_value == INT_LITERAL)
											{
												value_int = atoi(cur->tok_string);
											}
										else 
											{
												value_str = cur->tok_string;
											}
										//printf("%s",value_str);
										}
									num = table->num_records;
									nr=0;
									r = 0;
									os=0;
									int recosize =0;
									int colid =0,collen=0,colmat=0,value=0,coltype;
									for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
										{
											if(strcmp(colcollect,col_entry->col_name)==NULL)
												{
												colid = col_entry->col_id;
												//printf("%d\n Coll id",colid);
												collen = col_entry -> col_len;
												recosize = table->record_size;
												coltype = col_entry -> col_type;
												}
										}
									int offset = 24;
									while (nr<num)
										{
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(colid==col_entry->col_id)
														{
														if(offsetflag == false)
															{
																offset = offset + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																if(valtype == 15 && col_entry->not_null==1)
																{
																	printf("Trying to insert null in not null\n");
																}
																else if(valtype == 91)
																{
																	printf("Datatype Mismatch\n");
																}
																else
																	{
																fwrite(&value_int,sizeof(int),1,fc);
																	}
																//fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																offset = offset + recosize;
															}
														else
															{
																//printf("%s/n",value_str);
															if(valtype == 15 && col_entry->not_null==1)
																{
																	printf("Trying to insert null in not null\n");
																}
															else if(valtype == 90)
																{
																	printf("Datatype Mismatch\n");
																}
															else
																{
																fwrite(value_str,col_entry->col_len,1,fc);
																}
																//fread(&W_ValueStr,col_entry->col_len,1,fc);
																//printf("Value is %d \n",W_ValueStr);
																offset = offset + recosize;
															}
															offsetflag = true;
														}
													else
														{
															if(offsetflag==false)
																{
																	offset = offset + col_entry->col_len+1;
																}
														}
												} nr++;
										}
							}
							else if(foundwhere)
							{
							cur2 = cur2->next;
							if(cur2->tok_value == EOC )
								{
									rc = INVALID_UPDATE_STATEMENT_DEf;
									cur2->tok_value = INVALID;
									printf("Invalid Syntax after entering WHERE\n");
								}
							else
								{
									//Check to see if where column exists 
									int colid2 = 0;
									col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
									for(i = 0;i < tab_entry->num_columns; i++,col_entry++)
										{
										if(stricmp(cur2->tok_string,col_entry->col_name)==NULL)
											{
												foundWhereflag = true;
												printf("Updating Based on the column %s \n", col_entry->col_name);
												//colcollect = col_entry->col_name;
												colid2 = col_entry->col_id;
											}
										}
									if(foundWhereflag)
										{
										
										printf("Entered Execution Section for where clause\n");
										cur2=cur2->next;
										if(cur2->tok_value == EOC)
											{
												rc = INVALID_UPDATE_STATEMENT_DEf;
												printf("INVALID STATEMENT\n");
												cur2->tok_value = INVALID;
											}
										else if(cur2->tok_value == S_GREATER)
											{
												relationop = S_GREATER;
											}
										else if(cur2->tok_value == S_LESS)
											{
												relationop = S_LESS;
											}
										else if(cur2->tok_value == S_EQUAL)
											{
												relationop = S_EQUAL;
											}
										else
											{
												rc = INVALID_UPDATE_STATEMENT_DEf;
												printf("INVALID RELATIONAL OPERATOR\n");
												cur2->tok_value = INVALID;
											}
										cur2=cur2->next;
										if(cur2->tok_value == EOC)
											{
												rc = INVALID_UPDATE_STATEMENT_DEf;
												printf("INVALID STATEMENT\n");
												cur2->tok_value=INVALID;
											}
										else
											{
											//printf("%s",cur->tok_string);
											//Keeping Value to be updated in check
											cur = cur ->next -> next;
											valtype = cur->tok_value;
											if(cur->tok_value == INT_LITERAL)
												{
													A_Value = atoi(cur->tok_string);
												}
											else
												{
													A_ValueStr = cur->tok_string;
												}
											// Keeping The where condition value in check;
											if (cur2->tok_value == INT_LITERAL)
													{
													value_int = atoi(cur2->tok_string);
													}
												else
													{
													value_str = cur2->tok_string; 
													}
											//printf("%s",value_str);
											}
										num = table->num_records;
										nr=0;
										r = 0;
										os=0;
										int recosize1 = table->record_size;
										int colid =0,collen=0,colmat=0,value=0;
										//Here I will keep in mind the record number for which I will have to update
										
										int offset1 = 24;
										bool offsetflag1 = false;
										while (nr<num)
										{
											for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
												;j< tab_entry->num_columns;j++,col_entry++)
												{	
													if(colid2==col_entry->col_id)
														{
														if(offsetflag1 == false)
															{
																offset1 = offset1 + 1;
															}
														//printf ("coll id is %d\n",offset);
														fseek(fc,offset1,SEEK_SET);
														if(col_entry->col_type == T_INT)
															{
																//fwrite(&value_int,sizeof(int),1,fc);
																fread(&W_ValueInt,sizeof(int),1,fc);
																//printf("Value is %d \n",W_ValueInt);
																switch(relationop)
																{
																	case S_EQUAL:
																				if(W_ValueInt == value_int)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_GREATER:
																				if(W_ValueInt > value_int)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	case S_LESS:
																				if(W_ValueInt < value_int)
																					{
																						ListTrue[nr] = true;
																					}
																				else
																					{
																						ListTrue[nr] = false;
																					}
																				offset1 = offset1 + recosize1;
																				break;
																	default :
																		printf("Invalid Relational Operator\n");
																	}
															}
														else
															{
																//printf("%s/n",value_str);
																//fwrite(value_str,col_entry->col_len,1,fc);
																W_ValueStr = (char *)calloc(1,col_entry->col_len);
																memset(W_ValueStr, '\0', col_entry->col_len);
																fread(W_ValueStr,col_entry->col_len,1,fc);
																//printf("Value is %s \n",W_ValueStr);
																if(stricmp(W_ValueStr,value_str)==NULL)
																	{
																		ListTrue[nr] = true;
																	}
																else
																	{
																		ListTrue[nr] = false;
																	}
																offset1 = offset1 + recosize1;
															}
															offsetflag1 = true;
														}
													else
														{
															if(offsetflag1==false)
																{
																	offset1 = offset1 + col_entry->col_len+1;
																}
														}
												} nr++;
										}
									
										//Here I will update the record.
										for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
													;j< tab_entry->num_columns;j++,col_entry++)
											{
												if(stricmp(colcollect,col_entry->col_name)==NULL)
													{
													colid = col_entry->col_id;
													//printf("%d\n Coll id",colid);
													collen = col_entry -> col_len;
													recosize1 = table->record_size;
													}
											}
										int offset = 24;
										nr = 0;
										while (nr<num)
											{
												for (j = 0,col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset)
													;j< tab_entry->num_columns;j++,col_entry++)
													{	
														if(colid==col_entry->col_id)
															{
															if(offsetflag == false)
																{
																	offset = offset + 1;
																}
															//printf ("coll id is %d\n",offset);
															fseek(fc,offset,SEEK_SET);
															if(col_entry->col_type == T_INT)
																{
																	if(ListTrue[nr])
																	{
																	fwrite(&A_Value,sizeof(int),1,fc);
																	result++;
																	}
																	offset = offset + recosize1;
																}
															else
																{
																//printf("%s/n",value_str);
																	if(ListTrue[nr])
																	{
																	fwrite(A_ValueStr,col_entry->col_len,1,fc);
																	result++;
																	}
																	offset = offset + recosize1;
																}
																offsetflag = true;
															}
														else
															{
																if(offsetflag==false)
																	{
																		offset = offset + col_entry->col_len+1;
																	}
															}
													} nr++;
											}
										}
									else
										{
											rc = COLUMN_NOT_EXIST;
											cur2->tok_value = INVALID_STATEMENT;
											printf("Column mentioned after WHERE doesnot exists\n");
										}

								}
							}
							else
							{
								rc = COLUMN_NOT_FOUND;
								cur->tok_value = INVALID;
								printf("Column Not Found");
							}
				}

		}
		if(!rc)
			{
				printf("Update Successful\n");
				fclose(fc);
			}
		
	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN + 1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if ((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);
						}

						/* Next, write the cd_entry information */
						for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list)-sizeof(tpd_entry),
								1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								(sizeof(tpd_list)-sizeof(tpd_entry)),
								1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
							1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						has already subtracted the cur->tpd_size, therefore it will
						point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
								old_size - cur->tpd_size -
								((char*)cur - (char*)g_tpd_list),
								1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}


			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}
			
int add_entry_to_log(char *query)
{
	FILE *fc = NULL;
	char *entry = NULL, *point;
	char *cp;
	int rc = 0;
	int length=0;
	time_t t;
	struct tm *tinfo;
	char store[15];

	cp = (char *)malloc((strlen(query) + 1) *
			sizeof(char));
		strcpy(cp, query);

	length = strlen(query)+8;

	if ((fc = fopen("db.log", "a")) == NULL)
		{
		rc = FILE_OPEN_ERROR;
		}
	else
	{
		point = strtok(query, " ");
		if (stricmp(point, "backup") == 0)
		{
			entry = (char *)malloc((length) *sizeof(char));
			strcpy(entry, cp);
			fputs(entry, fc);
 		}
		else if (stricmp(query, "RF_START") == 0)
		{
			entry = (char *)malloc((strlen("RF_START") + 1) * sizeof(char));
			strcpy(entry, "RF_START\n");
			fputs(entry, fc);
		}
		else
		{
			entry = (char *)malloc((strlen(query) + 14 + 4) * 
				sizeof(char)); 

			time(&t);
			tinfo = localtime(&t);
			strftime(store, 15, "%Y%m%d%H%M%S", tinfo);

			//Pause for 2 seconds. 
			Sleep(2);
			
			strcpy(entry, store);
			strcat(entry, " \"");
			strcat(entry, cp);
			strcat(entry, "\"\n");
			fputs(entry, fc);
		}
		
		fclose(fc);
	}

	return rc;
}

int sem_backup(token_list *t_list)
	{
		 token_list *cur =0;
		 tpd_entry *t_cur = &(g_tpd_list->tpd_start);
		 FILE *fc = NULL,*fd=NULL;
		 struct _stat file_stat;
		 int len;
		 table_file_header *tab = NULL;
		 cur = t_list;
		 int rc =0,num=g_tpd_list->num_tables,i;
		 char *imgname;
		 char stmt[3000];
		 if (cur->tok_value != K_TO)
			{
				rc = INVALID_STATEMENT;
				cur->tok_value = INVALID;
				printf(" Wrong Syntax \n");
			}
		 else
			 {
				 cur = cur->next;
				 if(cur->tok_value == EOC)
					 {
						rc = INVALID_BACKUP_STATEMENT;
						cur->tok_value = INVALID;
						printf("Invalid backup syntax\n");
					 }
				 else
					 {
						imgname = cur->tok_string;
						cur = cur->next;
						if(cur->tok_value!=EOC)
							{
							rc = INVALID_BACKUP_STATEMENT;
							cur->tok_value = INVALID;
							printf("Invalid backup syntax");
							}
					 }
			 }

			if(!rc)
			{
				strcpy(stmt, "BACKUP ");
				strcat(stmt,imgname);
				strcat(stmt,"\n");
				rc = add_entry_to_log(stmt);

				if ((fc = fopen(imgname, "r")) != NULL)
				{
					printf("Image file already exists");
					rc = INVALID_IMAGE_FILE_NAME;
				}
				else if ((fc = fopen(imgname, "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}

				if(!rc)
					{
					fwrite(g_tpd_list, g_tpd_list->list_size, 1, fc);
					i = 0;
					while (i < num)
					{
						char fname[MAX_IDENT_LEN + 4]; //
						memset(fname, '\0', MAX_IDENT_LEN + 4);
						strcat(fname, t_cur->table_name);
						strcat(fname, ".tab");
						if ((fd = fopen(fname, "rbc")) == NULL)
						{
						
							printf("File cannot be oppened \n");
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							_fstat(_fileno(fd), &file_stat);
							tab = (table_file_header*) calloc(1, file_stat.st_size);
							memset(tab, '\0', file_stat.st_size);
							len = file_stat.st_size;
							fread(tab, file_stat.st_size, 1, fd);
							fclose(fd);
						}	
						if(!rc)
						{
							fwrite(&len, sizeof(int), 1, fc);
							fwrite(tab, len, 1, fc);
							t_cur = (tpd_entry*)((char*)t_cur + t_cur->tpd_size);
						}
						i++;
					}
				}
			}
			if(!rc)
				{
			fclose(fc);
				}
			return rc;
	}

int sem_restore(token_list *t_list)
	{
	FILE *fp=NULL,*fd=NULL,*IMG=NULL;
	int rc = 0;
	char imgname[MAX_IDENT_LEN] = {""};
	table_file_header *tab = NULL;
	row *ri = NULL;
	tpd_entry *t_cur = &(g_tpd_list->tpd_start);
	char statement[3000],olog[MAX_IDENT_LEN + 5];
	token_list *cur = NULL;
	tpd_entry *tp_cur = NULL;
	//tp_cur = (tpd_entry*)((char*)tp_cur + tp_cur->tpd_size);
	cur = t_list;
	if (cur->tok_value != K_FROM)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
		printf("Missing FROM Keyword\n");
	}
	else
	{
		cur = cur->next;
		if (cur->tok_value == EOC)
		{
			rc = INVALID_STATEMENT;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(imgname, cur->tok_string);
			if (cur->next->tok_value == EOC)
			{
						int i, rc = 0, size, num_tables, offset;
						char fname[MAX_IDENT_LEN + 5];
						if ((IMG = fopen(imgname, "r+bc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
							printf("Unable to open file\n");
						}
						else
						{
							size = g_tpd_list->list_size;
							tab = (table_file_header*) calloc(1, size);
							memset(tab, '\0', size);
							fread(tab, size, 1, IMG);
							if ((fp = fopen("dbfile.bin", "wbc")) == NULL)
							{
								printf("Unable to open file\n");
								rc = FILE_OPEN_ERROR;
							}
							else 
							{
								fwrite(tab,size,1,fp);
							}

							// loop through g_tpd_list and restore each table
							if (!rc)
							{
								num_tables = g_tpd_list->num_tables;
								for (i = 0, tp_cur = &(g_tpd_list->tpd_start); 
									i < num_tables; i++, tp_cur = (tpd_entry*)((char*)tp_cur + tp_cur->tpd_size))
								{
									size = 0;
									fread(&size, sizeof(int), 1, IMG);

									// get table name
									strcpy(fname, tp_cur->table_name);
									strcat(fname, ".tab\0");

									// read table data from backup image and write it to disk
									tab = (table_file_header *)malloc(size);
									memset(tab, '\0', size);
									fread(tab, size, 1, IMG);

									if ((fd = fopen(fname, "wbc")) == NULL)
										{
											rc = FILE_OPEN_ERROR;
										}
										else
										{
										fwrite(tab, tab->file_size, 1, fd);
										}
									fclose(fd);
									free(tab);
								}
							}
						}

						if (IMG)
							fclose(IMG);
						if (fp)
							fclose(fp);

						if (!rc)
						{
							printf("Successfully executed \n");
							FILE *fp = NULL;
							int rc = 0;

							if (g_tpd_list->db_flags == 0)
								g_tpd_list->db_flags = 1;
							else
								g_tpd_list->db_flags = 0;

							if ((fp = fopen("dbfile.bin", "wbc")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
							}
							else
							{
								fwrite(g_tpd_list, g_tpd_list->list_size, 1, fp);
								fclose(fp);
							}
							if(!rc)
								{
								add_entry_to_log("RF_START");
								}
						}
			}
			else
			{
				cur = cur->next;
				if (stricmp(cur->tok_string,"without")==NULL)
				{
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
					printf("Missing WITHOUT Keyword \n");
				}
				else
				{
					cur = cur->next;
					if (stricmp(cur->tok_string,"RF") != NULL ||
						cur->next->tok_value != EOC)
					{
						rc = INVALID_STATEMENT;
						cur->tok_value = INVALID;
						printf("Missing RF Keyword or Too Much Data\n");
					}
					else
					{
						//rc = restore(image_name, 0);
						int i, rc = 0, size, num_tables, offset;
						char fname[MAX_IDENT_LEN + 5];
						if ((IMG = fopen(imgname, "r+bc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
							printf("Unable to open file\n");
						}
						else
						{
							size = g_tpd_list->list_size;
							tab = (table_file_header*) calloc(1, size);
							memset(tab, '\0', size);
							fread(tab, size, 1, IMG);
							if ((fp = fopen("dbfile.bin", "wbc")) == NULL)
							{
								printf("Unable to open file\n");
								rc = FILE_OPEN_ERROR;
							}
							else 
							{
								fwrite(tab,size,1,fp);
							}

							// loop through g_tpd_list and restore each table
							if (!rc)
							{
								num_tables = g_tpd_list->num_tables;
								for (i = 0, tp_cur = &(g_tpd_list->tpd_start); 
									i < num_tables; i++, tp_cur = (tpd_entry*)((char*)tp_cur + tp_cur->tpd_size))
								{
									size = 0;
									fread(&size, sizeof(int), 1, IMG);

									// get table name
									strcpy(fname, tp_cur->table_name);
									strcat(fname, ".tab\0");

									// read table data from backup image and write it to disk
									tab = (table_file_header *)malloc(size);
									memset(tab, '\0', size);
									fread(tab, size, 1, IMG);

									if ((fd = fopen(fname, "wbc")) == NULL)
										{
											rc = FILE_OPEN_ERROR;
										}
										else
										{
										fwrite(tab, tab->file_size, 1, fd);
										}
									fclose(fd);
									free(tab);
								}
							}
						}

						if (IMG)
							fclose(IMG);
						if (fp)
							fclose(fp);

						if (!rc)
						{
							printf("Successfully executed, Also created a backup\n");
						}
						// create new log file and copy log entries to it
						if (!rc)
						{
							FILE *fp1,*fp2; int i =0; char fname1[MAX_IDENT_LEN + 5];
							char temp_str[MAX_IDENT_LEN]; bool cond = false; char a;
								i = 1;
								while (!cond)
								{
									// get new file name
									memset(fname1, '\0', MAX_IDENT_LEN);
									strcpy(fname1, "db.log");
									itoa(i, temp_str, 10);
									strcat(fname1, temp_str);
									// try to open file, if fopen returns NULL 
									// rename log file to the current file name!
									if ((fp1 = fopen(fname1, "r")) == NULL)
									{
										cond = true;
									}
									i++;
								}
								//fclose(fp1);
								if ((fp1 = fopen(fname1, "a")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								if ((fp2 = fopen("db.log", "r")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								if(!rc) {

								  do
									{
										a=fgetc(fp2);
										fputc(a,fp1);
									}while(a!=EOF);

									fclose(fp1);
									fclose(fp2);
									}
						}
					}
				}
			}
		}
		
	}
	return rc;
}





void  run_xact(DB_ENV *, DB *, int, int);
void *checkpoint_thread(void *);
void  log_archlist(DB_ENV *);
void *logfile_thread(void *);
/**
   @param type DB_RECNO, DB_HASH or DB_BTREE 
*/
void  db_open(DB_ENV *, DB **, char *, int type);
void  env_dir_create(void);
void  env_open(DB_ENV **);
void  usage(void);

extern DB_ENV *dbenv;
extern DB *db_cats;

#define	ENV_DIRECTORY	"TXNAPP"


"Transactional topic-aware schema migration. Call only with the gateway stopped."

SCHEMA = 6


def migrate(db, version):
    if version < 4:
        migrate_to_four(db, version)
    elif version > SCHEMA:
        raise ValueError("Unsupported database schema")
    if version < 5:
        migrate_to_five(db)
    if version < 6:
        migrate_to_six(db)


def migrate_to_six(db):
    try:
        db.executescript("""
        BEGIN IMMEDIATE;
        CREATE TABLE delegations(id INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT NOT NULL,
          route INTEGER NOT NULL,bot TEXT NOT NULL,task TEXT NOT NULL,status TEXT NOT NULL,
          answer TEXT,created REAL NOT NULL,finished REAL);
        CREATE UNIQUE INDEX one_task_per_bot ON delegations(instance,bot)
          WHERE status='running';
        PRAGMA user_version=6;
        COMMIT;
        """)
    except BaseException:
        db.rollback()
        raise


def migrate_to_five(db):
    try:
        db.executescript("""
        BEGIN IMMEDIATE;
        CREATE TABLE bots(id TEXT PRIMARY KEY,instance TEXT NOT NULL,name TEXT NOT NULL,
          name_key TEXT NOT NULL,role TEXT NOT NULL,instructions TEXT NOT NULL DEFAULT '',
          chief INTEGER NOT NULL DEFAULT 0,created REAL NOT NULL,UNIQUE(instance,name_key));
        CREATE UNIQUE INDEX one_chief ON bots(instance) WHERE chief=1;
        ALTER TABLE routes ADD COLUMN bot TEXT;
        CREATE TABLE route_bots(route INTEGER NOT NULL,bot TEXT NOT NULL,session TEXT,
          session_file TEXT,title TEXT,provider TEXT,model TEXT,thinking TEXT,
          PRIMARY KEY(route,bot));
        PRAGMA user_version=5;
        COMMIT;
        """)
    except BaseException:
        db.rollback()
        raise


def migrate_to_four(db, version):
    base = """
    CREATE TABLE IF NOT EXISTS instances(id TEXT PRIMARY KEY, bot_id INTEGER);
    CREATE TABLE IF NOT EXISTS poll_state(instance TEXT PRIMARY KEY, offset INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS session_status(
      instance TEXT, session TEXT, data TEXT, PRIMARY KEY(instance,session));
    """
    if version == 0:
        changes = """
        CREATE TABLE conversations(instance TEXT, chat INTEGER, user INTEGER, session TEXT,
          persisted INTEGER DEFAULT 0, thread INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(instance,chat,user,thread));
        CREATE TABLE inbox(instance TEXT,id INTEGER,chat INTEGER,user INTEGER,text TEXT,status TEXT,
          run TEXT,created REAL,thread INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(instance,id));
        CREATE TABLE approvals(instance TEXT,id TEXT,chat INTEGER,user INTEGER,run TEXT,tool TEXT,
          digest TEXT,created REAL,expires REAL,decision TEXT,source TEXT,
          thread INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(instance,id));
        CREATE TABLE session_owners(instance TEXT,session TEXT,chat INTEGER,user INTEGER,title TEXT,
          provider TEXT,model TEXT,thinking TEXT,created REAL,updated REAL,
          thread INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(instance,session));
        """
    elif version in (1, 2):
        changes = """
        CREATE TABLE IF NOT EXISTS session_owners(instance TEXT,session TEXT,
          chat INTEGER,user INTEGER,
          title TEXT,provider TEXT,model TEXT,thinking TEXT,created REAL,updated REAL,
          PRIMARY KEY(instance,session));
        ALTER TABLE conversations RENAME TO conversations_previous;
        CREATE TABLE conversations(instance TEXT,chat INTEGER,user INTEGER,session TEXT,
          persisted INTEGER DEFAULT 0,thread INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY(instance,chat,user,thread));
        INSERT INTO conversations(instance,chat,user,session,persisted,thread)
          SELECT instance,chat,user,session,persisted,0 FROM conversations_previous;
        DROP TABLE conversations_previous;
        ALTER TABLE inbox ADD COLUMN thread INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE approvals ADD COLUMN thread INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE session_owners ADD COLUMN thread INTEGER NOT NULL DEFAULT 0;
        """
    elif version == 3:
        changes = ""
    else:
        raise ValueError("Unsupported database schema")
    seed = """
    INSERT OR IGNORE INTO session_owners(instance,session,chat,user,thread,created,updated)
      SELECT instance,session,chat,user,thread,0,0
      FROM conversations WHERE session IS NOT NULL;
    """
    additions = """
    ALTER TABLE inbox ADD COLUMN route_user INTEGER;
    UPDATE inbox SET route_user=user;
    CREATE TABLE routes(id INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT NOT NULL,
      chat INTEGER NOT NULL,user INTEGER NOT NULL,thread INTEGER NOT NULL DEFAULT 0,
      session TEXT,session_file TEXT,title TEXT,provider TEXT,model TEXT,thinking TEXT,
      created REAL NOT NULL,updated REAL NOT NULL,UNIQUE(instance,chat,user,thread));
    CREATE TABLE messages(id INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT NOT NULL,
      route INTEGER NOT NULL,direction TEXT NOT NULL,text TEXT,platform_id TEXT,created REAL,
      UNIQUE(instance,direction,platform_id));
    CREATE TABLE instance_archive(instance TEXT PRIMARY KEY,archived REAL);
    CREATE TABLE imports(instance TEXT,source TEXT,fingerprint TEXT,completed REAL,
      PRIMARY KEY(instance,source));
    CREATE INDEX inbox_pending ON inbox(instance,status,id);
    CREATE INDEX routes_sessions ON routes(instance,session_file);
    """
    try:
        db.executescript(
            "BEGIN IMMEDIATE;\n"
            + base
            + changes
            + seed
            + additions
            + "PRAGMA user_version=4; COMMIT;"
        )
    except BaseException:
        db.rollback()
        raise

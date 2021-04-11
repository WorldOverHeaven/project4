import sqlite3
THESAURUS_PATH = "C:\\Users\\Kirill\\PycharmProjects\\pythonProject\\rutez.db"


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return conn


with create_connection(THESAURUS_PATH) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE hyperonyms;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hyperonyms(
        id INTEGER,
        name VARCHAR(20),
        link INTEGER,
        linkname VARCHAR(20));
    """)
    conn.commit()

# -*- coding: utf-8 -*-
# By Dmitry Demidov
import sqlite3
import time
#from LP.dict_helper import THESAURUS_PATH
THESAURUS_PATH = "rutez.db"


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


def concept_by_name(conn, lemma):
    """
    returns list of concepts containing -word- as a synonym
    :param conn: Connection object returned by create_connection
    :param lemma: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM word, sinset "
                "WHERE word.name=? and sinset.id=word.id", (lemma.upper(),))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def synonyms_by_id(conn, id):
    """
    returns list of synonyms as a comma separated string
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT name FROM word WHERE id=?", (id,))

    rows = cur.fetchall()
    return [row[0] for row in rows]


def relations_by_id(conn, id):
    """
    returns list of relations for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT rel.link, sinset.name, rel.name FROM rel, sinset "
                "WHERE rel.id=? and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1], row[2],))

    return res


def hyperonyms_by_id(conn, id):
    """
    returns list of direct hyperonyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM rel, sinset "
                "WHERE rel.id=? and rel.name='ВЫШЕ' and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def all_hyperonyms_by_id(conn, id):
    """
    returns list of ALL hyperonyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute(
        "WITH RECURSIVE hyperonyms(id) AS "
        "(VALUES(?) UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
        "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (id,)
    )
    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def all_hyperonyms_by_name(conn, lemma):
    """
    returns list of ALL RECURSIVE hyperonyms for -lemma-
    :param conn: Connection object returned by create_connection
    :param lemma: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute(
        "WITH RECURSIVE hyperonyms(id) AS "
        "(SELECT sinset.id FROM word, sinset WHERE word.name=? and sinset.id=word.id "
        "UNION SELECT rel.link FROM rel, hyperonyms WHERE rel.name='ВЫШЕ' and rel.id=hyperonyms.id) "
        "SELECT sinset.id, sinset.name FROM sinset, hyperonyms WHERE hyperonyms.id = sinset.id", (lemma.upper(),)
    )
    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def hyponyms_by_id(conn, id):
    """
    returns list of hyponyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM rel, sinset "
                "WHERE rel.id=? and rel.name='НИЖЕ' and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def meronyms_by_id(conn, id):
    """
    returns list of meronyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM rel, sinset "
                "WHERE rel.id=? and rel.name='ЧАСТЬ' and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def holonyms_by_id(conn, id):
    """
    returns list of holonyms for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM rel, sinset "
                "WHERE rel.id=? and rel.name='ЦЕЛОЕ' and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def associations_by_id(conn, id):
    """
    returns list of associations for -id-
    :param conn: Connection object returned by create_connection
    :param id: str
    :return: list of (integer, str)
    """
    cur = conn.cursor()
    cur.execute("SELECT sinset.id, sinset.name FROM rel, sinset "
                "WHERE rel.id=? and rel.name='АССОЦ' and rel.link=sinset.id", (id,))

    rows = cur.fetchall()
    res = []
    for row in rows:
        res.append((row[0], row[1],))

    return res


def main():
    lemma = 'постоянная сущность'
    with create_connection(THESAURUS_PATH) as conn:
        print("get_words_by_name:")
        for id, concept in concept_by_name(conn, lemma):
            print("{} {}".format(id, concept))
            print("  synonyms: {}".format(synonyms_by_id(conn, id)))

            print('  hyperonyms:')
            for wid, word in hyperonyms_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

            print('  all hyperonyms:')
            for wid, word in all_hyperonyms_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

            print('  hyponyms:')
            for wid, word in hyponyms_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

            print('  holonyms:')
            for wid, word in holonyms_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

            print('  meronyms:')
            for wid, word in meronyms_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

            print('  associations:')
            for wid, word in associations_by_id(conn, id):
                print("    {: >5} {}".format(wid, word))

        print('\nall hyperonyms by name:')
        for wid, word in all_hyperonyms_by_name(conn, lemma):
            print("  {: >5} {}".format(wid, word))


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print('%r  %2.2f ms' % (method.__name__, (te - ts) * 1000))
        return result

    return timed


def test_time2():
    lemma = 'явление'
    t0 = time.time()
    with create_connection(THESAURUS_PATH) as conn:
        a = all_hyperonyms_by_name(conn, lemma)
    t1 = time.time()
    print(t1-t0)



@timeit
def test():
    lemma = 'явление'
    with create_connection(THESAURUS_PATH) as conn:
        a = all_hyperonyms_by_name(conn, lemma)


if __name__ == '__main__':
    test()


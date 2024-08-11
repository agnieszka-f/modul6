import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_book(conn, book):
    sql = '''INSERT INTO books(id, title, author, isbn) VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, book)
    conn.commit()

def add_loan(conn, loan):
    sql = '''INSERT INTO loans(loanId, loanDate, returnDate, friendName, bookId) VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, loan)
    conn.commit()

def insert_data(list, insert_func, conn):
    for item in list:
        insert_func(conn, item)

def select_all(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    return rows

def select_where(conn, table, columns, **query):
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT {columns} FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows

def update(conn, table, id, **kwargs):
    parameters = [ f"{k}=?" for k in kwargs ]
    parameters = ", ".join(parameters)
    values = tuple( v for v in kwargs.values())
    values += (id,)
    sql = f'''UPDATE {table} SET {parameters} WHERE id=?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
    except sqlite3.OperationalError as e:
        print(e)

sql_books = """
CREATE TABLE IF NOT EXISTS books (
    id integer PRIMARY KEY,
    title text NOT NULL,
    author text NOT NULL,
    isbn varchar(18)
);
"""

sql_loans = """
CREATE TABLE IF NOT EXISTS loans (
    id integer PRIMARY KEY,
    loanDate text,
    returnDate text,
    friendName text,
    bookId integer,
    FOREIGN KEY (bookId) REFERENCES books (id)
);
"""

db_file = "books_1.db"

books = [
    (1, "Władca Pierścieni", "J.R.R. Tolkien", "978-83-123456-7-8"),
    (2, "1984", "George Orwell", "978-83-876543-2-1"),
    (3, "Hobbit", "J.R.R. Tolkien", "978-83-765432-1-2"),
    (4, "Harry Potter i Kamień Filozoficzny", "J.K. Rowling", "978-83-123765-4-3"),
    (5, "Zbrodnia i kara", "Fiodor Dostojewski", "978-83-908765-1-4")
]

loans = [
    (1, "2024-07-01", None, "Michał Nowak", 1),
    (2, "2024-07-10", "2024-07-20", "Anna Kowalska", 2),
    (3, "2024-08-01", None, "Katarzyna Zielińska", 3),
    (4, "2024-08-05", None, "Tomasz Wiśniewski", 4),
    (5, "2024-08-10", "2024-08-15", "Paweł Jabłoński", 5),
    (6, "2024-08-12", None, "Michał Nowak", 4),
    (7, "2024-08-15", None, "Anna Kowalska", 1)
]

if __name__ == "__main__":
    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, sql_books)
        execute_sql(conn, sql_loans)
        insert_data(books, add_book, conn)
        insert_data(loans, add_loan, conn)
        all_books = select_all(conn, "books")
        all_loans = select_all(conn, "loans")
        print(all_books)
        print(all_loans)
        res1 = select_where(conn, "books", "title", author="J.R.R. Tolkien")
        res2 = select_where(conn, "loans", "*", bookId=4, friendName="Tomasz Wiśniewski")
        print(res1)
        print(res2)
        update(conn, "loans", 7, returnDate="2024-08-20")
        conn.close()
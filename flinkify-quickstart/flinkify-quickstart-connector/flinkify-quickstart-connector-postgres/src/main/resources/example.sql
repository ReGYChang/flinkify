-- Create the authors table
CREATE TABLE authors
(
    author_id   SERIAL PRIMARY KEY,
    first_name  VARCHAR(50) NOT NULL,
    last_name   VARCHAR(50) NOT NULL,
    birth_date  DATE,
    nationality VARCHAR(50)
);

-- Insert data into authors table
INSERT INTO authors (first_name, last_name, birth_date, nationality)
VALUES ('J.K.', 'Rowling', '1965-07-31', 'British'),
       ('George R.R.', 'Martin', '1948-09-20', 'American'),
       ('Haruki', 'Murakami', '1949-01-12', 'Japanese'),
       ('Jane', 'Austen', '1775-12-16', 'British');

-- Create the books table
CREATE TABLE books
(
    book_id          SERIAL PRIMARY KEY,
    title            VARCHAR(100)       NOT NULL,
    author_id        INTEGER REFERENCES authors (author_id),
    publication_date DATE,
    isbn             VARCHAR(13) UNIQUE NOT NULL,
    price            DECIMAL(10, 2)     NOT NULL,
    stock            INTEGER            NOT NULL DEFAULT 0
);

-- Insert data into books table
INSERT INTO books (title, author_id, publication_date, isbn, price, stock)
VALUES ('Harry Potter and the Philosopher''s Stone', 1, '1997-06-26', '9780747532699', 19.99, 100),
       ('A Game of Thrones', 2, '1996-08-01', '9780553103540', 24.99, 75),
       ('Norwegian Wood', 3, '1987-08-04', '9780375704024', 15.99, 50),
       ('Pride and Prejudice', 4, '1813-01-28', '9780141439518', 9.99, 120),
       ('1Q84', 3, '2009-05-29', '9780307593313', 22.99, 60);

-- Create the orders table
CREATE TABLE orders
(
    order_id      SERIAL PRIMARY KEY,
    book_id       INTEGER REFERENCES books (book_id),
    customer_name VARCHAR(100)   NOT NULL,
    order_date    DATE           NOT NULL DEFAULT CURRENT_DATE,
    quantity      INTEGER        NOT NULL,
    total_price   DECIMAL(10, 2) NOT NULL
);

-- Insert data into orders table
INSERT INTO orders (book_id, customer_name, order_date, quantity, total_price)
VALUES (1, 'Alice Johnson', '2023-05-15', 2, 39.98),
       (2, 'Bob Smith', '2023-05-16', 1, 24.99),
       (3, 'Charlie Brown', '2023-05-17', 3, 47.97),
       (4, 'Diana Prince', '2023-05-18', 1, 9.99),
       (5, 'Ethan Hunt', '2023-05-19', 2, 45.98);

CREATE TABLE inventory
(
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    description VARCHAR(100),
    quantity    INTEGER,
    price       DECIMAL(10, 2)
);

INSERT INTO inventory (name, description, quantity, price)
VALUES ('Widget A', 'A small widget for general use', 100, 9.99),
       ('Gadget B', 'An electronic gadget with multiple functions', 50, 24.95),
       ('Tool C', 'Heavy-duty tool for professional use', 30, 79.50),
       ('Accessory D', 'Complementary accessory for Widget A', 200, 4.99),
       ('Kit E', 'Complete set of Gadget B and accessories', 25, 49.99);
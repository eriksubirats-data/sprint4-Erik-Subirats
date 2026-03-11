##############NIVELL 1######################################################################################################################################################################################
# Creació database 'transactions_sprint4'
CREATE DATABASE IF NOT EXISTS transactions_sprint4;
  USE transactions_sprint4;

#Creació taula 'companies'
CREATE TABLE IF NOT EXISTS companies (
        id VARCHAR(15) PRIMARY KEY,
        company_name VARCHAR(255),
        phone VARCHAR(15),
        email VARCHAR(100),
        country VARCHAR(100),
        website VARCHAR(255)
    );
    
    SHOW GLOBAL VARIABLES LIKE 'local_infile';
    SHOW VARIABLES LIKE 'secure_file_prive';

#Insertar dades a taula 'companies'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/companies.csv'
INTO TABLE companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, company_name, phone, email, country, website);

#Creació taula 'credit_cards'
CREATE TABLE  IF NOT EXISTS credit_cards (
    id VARCHAR(15) PRIMARY KEY UNIQUE,
    user_id  INT,
    iban VARCHAR(50),
    pan VARCHAR(20),
    pin VARCHAR(4),
    cvv VARCHAR(3),
    track1 VARCHAR(100),
    track2 VARCHAR(100),
    expiring_date VARCHAR(8)
);
#Insertar dades a taula 'credit_cards'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/credit_cards.csv'
INTO TABLE credit_cards
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, user_id, iban, pan, pin, cvv, track1, track2, expiring_date);

#Creació taula 'european_users'
CREATE TABLE IF NOT EXISTS european_users (
	id INT PRIMARY KEY,
	name VARCHAR(100),
	surname VARCHAR(100),
	phone VARCHAR(150),
	email VARCHAR(150),
	birth_date VARCHAR(100),
	country VARCHAR(150),
	city VARCHAR(150),
	postal_code VARCHAR(100),
	address VARCHAR(255)    
);
#Insertar dades a taula 'european_users'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/european_users.csv'
INTO TABLE european_users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name, surname, phone, email, birth_date, country, city, postal_code, address);

#Creació taula 'american_users'
CREATE TABLE IF NOT EXISTS american_users (
	id INT PRIMARY KEY,
	name VARCHAR(100),
	surname VARCHAR(100),
	phone VARCHAR(150),
	email VARCHAR(150),
	birth_date VARCHAR(100),
	country VARCHAR(150),
	city VARCHAR(150),
	postal_code VARCHAR(100),
	address VARCHAR(255)    
);
#Insertar dades a taula 'american_users'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/american_users.csv'
INTO TABLE american_users
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name, surname, phone, email, birth_date, country, city, postal_code, address);

#Creació taula 'users'
CREATE TABLE IF NOT EXISTS users (
	id INT PRIMARY KEY,
	name VARCHAR(100),
	surname VARCHAR(100),
	phone VARCHAR(150),
	email VARCHAR(150),
	birth_date VARCHAR(100),
	country VARCHAR(150),
	city VARCHAR(150),
	postal_code VARCHAR(100),
	address VARCHAR(255)    
);

# Inserto tant 'american_users' com 'european_users' en una sola taula 'users'
INSERT INTO users
SELECT id, name, surname, phone, email, birth_date, country, city, postal_code, address
FROM european_users;
INSERT INTO users
SELECT id, name, surname, phone, email, birth_date, country, city, postal_code, address
FROM american_users;

#Elimino les taules 'american_users' i 'european_users'
DROP TABLE IF EXISTS american_users;
DROP TABLE IF EXISTS european_users;

#Creació taula 'products'
CREATE TABLE IF NOT EXISTS products (
	id INT PRIMARY KEY,
	product_name VARCHAR(100),
	price VARCHAR(100),
	colour VARCHAR(150),
	weight FLOAT,
	warehouse_id CHAR(10)
);
#Insertar dades a taula 'products'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/products.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, product_name, price, colour, weight, warehouse_id);

#Creació taula 'transactions'
 CREATE TABLE IF NOT EXISTS transactions (
        id VARCHAR(255) PRIMARY KEY,
        card_id VARCHAR(15),
        business_id VARCHAR(15), 
		timestamp TIMESTAMP,
        amount DECIMAL(10, 2),
        declined BOOLEAN,
        product_ids VARCHAR(100),
        user_id INT,
        lat FLOAT,
        longitude FLOAT,
        FOREIGN KEY (business_id) REFERENCES companies(id),
        FOREIGN KEY (card_id) REFERENCES credit_cards(id),
        FOREIGN KEY (user_id) REFERENCES users(id)
    );
    
    #Insertar dades a taula 'transactions'
LOAD DATA LOCAL INFILE 'C:/Users/eriks/Desktop/ADD/SPRINT 4/transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, card_id, business_id, timestamp, amount, declined, product_ids, user_id, lat, longitude);

##EXERCICI 1-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
	u.id, 
    u.name, 
    u.surname
FROM users u
WHERE u.id IN (
	SELECT t.user_id
    FROM transactions t
	GROUP BY t.user_id
	HAVING COUNT(t.id) > 80
);

##EXERCICI 2-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
	cc.iban AS IBAN, 
	AVG(t.amount) AS avg_amount
FROM credit_cards cc
JOIN transactions t ON cc.id = t.card_id
JOIN companies c ON t.business_id = c.id
WHERE c.company_name = 'Donec Ltd'
	AND t.declined = 0
GROUP BY cc.id, cc.iban;

##############NIVELL 2######################################################################################################################################################################################
#Creació taula 'card_activity'
CREATE TABLE IF NOT EXISTS card_activity (
    card_id VARCHAR(15) PRIMARY KEY,
    activity VARCHAR(10)
);
# Inserto les dades a 'card_activity'
INSERT INTO card_activity (card_id, activity)
SELECT 
    card_id,
    CASE 
        WHEN SUM(declined) = 3 THEN 'INACTIVE'
        ELSE 'ACTIVE'
    END AS activity
FROM (
    SELECT 
        card_id,
        declined,
        ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS rn
    FROM transactions
) t
WHERE rn IN (1, 2, 3)
GROUP BY card_id;
#Relaciono amb la taula 'credit_cards'
ALTER TABLE card_activity
ADD FOREIGN KEY (card_id) REFERENCES credit_cards(id);

##EXERCICI 1-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT COUNT(*) AS active_cards
FROM card_activity
WHERE activity = 'ACTIVE';

##############NIVELL 3######################################################################################################################################################################################
#Creació de la taula 'transaction_products'
CREATE TABLE transaction_products (
    transaction_id VARCHAR(255),
    product_id INT,
    FOREIGN KEY (transaction_id) REFERENCES transactions(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);
#Saber el número de productes més llarg:
SELECT 
	product_ids, 
    CHAR_LENGTH(product_ids) AS longitud
FROM transactions
ORDER BY longitud DESC
LIMIT 1;
# Inserto les dades a 'transaction_products'
INSERT INTO transaction_products 
SELECT DISTINCT transaction_id, product_id
FROM (
    SELECT id AS transaction_id, TRIM(SUBSTRING_INDEX(product_ids, ',', 1)) AS product_id
    FROM transactions

    UNION ALL

    SELECT id AS transaction_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', 2), ',', -1)) AS product_id
    FROM transactions
   
    UNION ALL

    SELECT id AS transaction_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', 3), ',', -1)) AS product_id
    FROM transactions
 
    UNION ALL

    SELECT id AS transaction_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', 4), ',', -1)) AS product_id
    FROM transactions

    UNION ALL

    SELECT id AS transaction_id, TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(product_ids, ',', 5), ',', -1)) AS product_id
    FROM transactions
) AS t;

##EXERCICI 1-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT 
	p.product_name, 
	COUNT(*) AS total_sales
FROM transaction_products tp
JOIN products p ON tp.product_id = p.id
JOIN transactions t ON tp.transaction_id = t.id
WHERE declined = 0
GROUP BY tp.product_id, p.product_name
ORDER BY total_sales DESC;
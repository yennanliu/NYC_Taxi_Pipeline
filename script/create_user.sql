-- create new mysql user 
CREATE USER 'mysql_user'@'localhost' IDENTIFIED BY '0000';
-- grant access 
GRANT ALL PRIVILEGES ON * . * TO 'mysql_user'@'localhost';
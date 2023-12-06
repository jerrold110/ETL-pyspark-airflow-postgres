-- Creating the parent table
CREATE TABLE parent_table (
    parent_id INT PRIMARY KEY,
    parent_data VARCHAR(255)
);

-- Creating the child table with a foreign key referencing the non-unique column
CREATE TABLE child_table (
    child_id INT PRIMARY KEY,
    parent_id INT,
    child_data VARCHAR(255),
    parent_data VARCHAR(255),
    FOREIGN KEY (parent_id) REFERENCES parent_table(parent_data)
);

CSV to JSON Converter (Assignment)
🧾 Overview

This Node.js + Express application reads data from a CSV file, converts it into JSON, and inserts it into a PostgreSQL database.
After insertion, it computes and displays the age distribution of all records.

⚙️ Technologies Used

Node.js

Express.js

PostgreSQL

dotenv

pg

📂 Project Structure
csv-json-converter/
│
|
├── index.js          # Main server file
├── db.js             # Database connection and queries
├── csvProcessor.js   # CSV parsing and insertion logic
│
├── data/
│   └── input_large.csv   # Test CSV file (50,000 rows)
│
├── .env
├── .gitignore
└── package.json

🧰 Setup Instructions

Install dependencies

npm install


Create .env file

DB_USER=your_postgres_user
DB_PASS=your_postgres_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=converter
CSV_FILE_PATH=./data/input_large.csv
BATCH_SIZE=1000
PORT=3000


Create database table

CREATE TABLE public.users (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  age INT NOT NULL,
  address JSONB,
  additional_info JSONB
);


Run the application

npm start


Trigger processing
Use Postman:

POST http://localhost:3000/process

📊 Example Output (Console)
CSV processing finished. Total records inserted: 50000.
===== Age-Group % Distribution =====
<20: 15%
20-40: 38%
40-60: 30%
>60: 17%
====================================

📝 Notes

.env and node_modules are excluded from GitHub.

The CSV file is not pushed due to its large size.

This project was created as part of an assignment on Node.js and PostgreSQL integration.

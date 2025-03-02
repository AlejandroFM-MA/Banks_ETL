# 🏦 Data Engineering Project - Banks Data ETL

This README provides a detailed explanation of the `banks_project.py` script and the ETL (Extract, Transform, Load) process implemented to analyze the largest banks worldwide.

## 📌 Project Overview

This project extracts data from Wikipedia, transforms it using exchange rates, and loads it into an SQLite database. The process follows the ETL pipeline structure:

1. **Extract:** Scrape data from a Wikipedia page.
2. **Transform:** Convert market capitalization from USD to other currencies.
3. **Load:** Store the transformed data in both a CSV file and an SQLite database.
4. **Query:** Run SQL queries to analyze the data.

## 💂‍♂️ Project Structure

- **`banks_project.py`** - Python script implementing the ETL pipeline.
- **`Banks.db`** - SQLite database storing processed data.
- **`Largest_banks_data.csv`** - Extracted and transformed data stored in CSV format.
- **`exchange_rate.csv`** - Exchange rates for currency conversion.
- **`code_log.txt`** - Log file documenting script execution.
- **`Images/`** - Folder containing screenshots required for the course.

---

## 👀 Code Breakdown

### 1️⃣ Logging Progress

```python
def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_message = f"{timestamp} : {message}\n"
    with open("code_log.txt", "a") as log_file:
        log_file.write(log_message)
```
- Appends timestamped messages to `code_log.txt` to track script execution.

### 2️⃣ Extracting Data

```python
def extract(url, table_attribs):
    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    tables = data.find_all('tbody')[0]
    rows = tables.find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            anchor_data = col[1].find_all('a')[1]
            if anchor_data is not None:
                data_dict = {'Name': anchor_data.contents[0], 'MC_USD_Billion': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    df['MC_USD_Billion'] = [float(x.strip()) for x in df['MC_USD_Billion']]
    return df
```
- Fetches HTML content from Wikipedia and parses it with BeautifulSoup.
- Extracts bank names and market capitalization values (in USD).

### 3️⃣ Transforming Data

```python
def transform(df, exchange_rate_path):
    exchange_csv = pd.read_csv(exchange_rate_path)
    rates = exchange_csv.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * rates['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * rates['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * rates['INR'], 2)
    return df
```
- Reads exchange rates from CSV.
- Converts market capitalization from USD to GBP, EUR, and INR.

### 4️⃣ Loading Data

```python
def load_to_csv(df, output_path):
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
```
- Saves transformed data into `Largest_banks_data.csv`.
- Loads data into SQLite database (`Banks.db`).

### 5️⃣ Querying Data

```python
def run_query(query_statements, sql_connection):
    for query in query_statements:
        print(pd.read_sql(query, sql_connection), '\n')
```
- Executes predefined SQL queries on the database.

---

## 🛠️ Running the ETL Pipeline

```python
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
exchange_rate_path = 'exchange_rate.csv'
table_attribs = ["Name", "MC_USD_Billion"]
output_path = 'Largest_banks_data.csv'
conn_Bank_db = sqlite3.connect('Banks.db')
table_name = 'Largest_banks'
log_file = 'code_log.txt'
query_statements = [
    'SELECT * FROM Largest_banks',
    'SELECT AVG(MC_GBP_Billion) FROM Largest_banks',
    'SELECT Name FROM Largest_banks LIMIT 5'
]

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df, exchange_rate_path)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df, output_path)
log_progress("Data saved to CSV file")
log_progress("SQL Connection initiated")
load_to_db(df, conn_Bank_db, table_name)
log_progress("Data loaded to Database. Running queries.")
run_query(query_statements, conn_Bank_db)
conn_Bank_db.close()
log_progress("Process Complete.")
```

---

## 📊 Sample Output

**Top 5 Largest Banks (MC_GBP_Billion):**

| Name          | MC_GBP_Billion |
|--------------|----------------|
| ICBC         | 250.5          |
| China C.Bank | 210.3          |
| JPMorgan     | 200.8          |
| HSBC         | 180.4          |
| Wells Fargo  | 175.2          |

**Average Market Capitalization in GBP:** `203.44`

---

## 🖼️ Project Screenshots

All images are stored inside the `Images/` folder:

- **Task 1:** Log Function (`Task_1_log_function.PNG`)
- **Task 2:** Data Extraction (`Task_2a_extract.png`, `Task_2b_extract.png`, `Task_2c_extract.png`)
- **Task 3:** Data Transformation (`Task_3a_transform.png`)
- **Task 4 & 5:** Save File (`Task_4_5_save_file.png`)
- **Task 6:** SQL Queries (`Task_6_SQL.png`)
- **Task 7:** Log Content (`Task_7_log_content.png`)

---

## 🚀 Conclusion

This project demonstrates:
- Web scraping with **BeautifulSoup**.
- Data transformation using **Pandas & NumPy**.
- Storage and retrieval using **SQLite**.
- Logging and debugging with **code_log.txt**.

Check out all my projects on my [GitHub](https://github.com/AlejandroFM-MA).



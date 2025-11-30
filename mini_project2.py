### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error
import csv
import datetime

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def _read_lines(data_filename):
    lines = []
    with open(data_filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            fields = line.split("\t")
            if fields[0].lower() == 'name' and 'productname' in line.lower():
                continue
            lines.append(line)
    return lines     


def step1_create_region_table(data_filename, normalized_database_filename):
    regions = set()

    with open(data_filename, "r", encoding="utf-8") as f:
        next(f)  # skip header
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) >= 5:
                region = cols[4].strip()
                if region:
                    regions.add(region)

    # Sort alphabetically as tests expect
    regions = sorted(list(regions))

    conn = create_connection(normalized_database_filename, delete_db=True)

    create_table(conn, """
        CREATE TABLE Region(
            RegionID INTEGER PRIMARY KEY AUTOINCREMENT,
            Region TEXT NOT NULL UNIQUE
        );
    """, drop_table_name="Region")

    with conn:
        conn.executemany(
            "INSERT INTO Region(Region) VALUES (?);",
            [(r,) for r in regions]
        )

    conn.close()
         

def step2_create_region_to_regionid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(
        "SELECT RegionID, Region FROM Region ORDER BY RegionID;",
        conn
    )
    region_dict = {row[1]: row[0] for row in rows}
    conn.close()
    return region_dict

  

def step3_create_country_table(data_filename, normalized_database_filename):
    pairs = set()

    with open(data_filename, "r", encoding="utf-8") as f:
        next(f)
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) >= 5:
                country = cols[3].strip()
                region = cols[4].strip()
                if country and region:
                    pairs.add((country, region))

    # Sort alphabetically BY country (required by test)
    pairs = sorted(list(pairs), key=lambda x: x[0])

    region_map = step2_create_region_to_regionid_dictionary(normalized_database_filename)

    conn = create_connection(normalized_database_filename)

    create_table(conn, """
        CREATE TABLE Country(
            CountryID INTEGER PRIMARY KEY AUTOINCREMENT,
            Country TEXT NOT NULL,
            RegionID INTEGER NOT NULL,
            FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
        );
    """, drop_table_name="Country")

    values = [(c, region_map[r]) for c, r in pairs]

    with conn:
        conn.executemany(
            "INSERT INTO Country(Country, RegionID) VALUES (?, ?);",
            values
        )

    conn.close()


def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    conn = create_connection(normalized_database_filename)
    rows = execute_sql_statement(
        "SELECT CountryID, Country FROM Country ORDER BY CountryID;",
        conn
    )
    country_dict = {row[1]: row[0] for row in rows}
    conn.close()
    return country_dict


      
def step5_create_customer_table(data_filename, normalized_database_filename):

  # WRITE YOUR CODE HERE
  country_to_countryid_dict=step4_create_country_to_countryid_dictionary(normalized_database_filename)

  lines=_read_lines(data_filename)
  customers=set()

  for line in lines:
    parts=line.split("\t")
    if len(parts)>=5:
      name=parts[0].strip()
      address=parts[1].strip()
      city=parts[2].strip()
      country=parts[3].strip()

      if name and address and city and country:
        name_parts=name.split()
        first_name=name_parts[0]
        last_name=" ".join(name_parts[1:]) if len(name_parts)>1 else ""

        country_id=country_to_countryid_dict.get(country)
        if country_id:
          customers.add((first_name, last_name, address, city, country_id))

  customers=sorted(customers, key=lambda x: (x[0],x[1]))

  conn=create_connection(normalized_database_filename)
  create_table_sql="""
  CREATE TABLE IF NOT EXISTS Customer(
    CustomerID INTEGER PRIMARY KEY AUTOINCREMENT,
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    Address TEXT NOT NULL,
    City TEXT NOT NULL,
    CountryID INTEGER NOT NULL,
    FOREIGN KEY(CountryID) REFERENCES Country(CountryID)
  );"""  

  create_table(conn, create_table_sql, drop_table_name="Customer")

  with conn:
    conn.executemany(
      "INSERT INTO Customer (FirstName, LastName, Address, City, CountryID) VALUES (?, ?, ?, ?, ?);",
      customers
    )

  conn.close()        

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):

# WRITE YOUR CODE HERE
  conn=sqlite3.connect(normalized_database_filename)
  cur=conn.cursor()

  cur.execute("SELECT CustomerID, FirstName, LastName FROM Customer;")
  rows=cur.fetchall()
  conn.close()

  customer_to_customerid_dict={}
  for customer_id, first_name, last_name in rows:
    full_name=f"{first_name} {last_name}".strip()
    customer_to_customerid_dict[full_name]=customer_id

  return customer_to_customerid_dict  


def step7_create_productcategory_table(data_filename, normalized_database_filename):
  
    # Inputs: Name of the data and normalized database filename
    # Output: None

    
# WRITE YOUR CODE HERE
  lines=_read_lines(data_filename)
  categories=set()

  for line in lines:
    parts=line.split("\t")
    if len(parts)>=8:

      product_categories=parts[6].split(";")
      product_category_desc=parts[7].split(";")

      for cat, desc in zip(product_categories, product_category_desc):
        cat=cat.strip()
        desc=desc.strip()
        if cat and desc:
          categories.add((cat, desc))


  categories=sorted(categories, key=lambda x: x[0])

  conn=create_connection(normalized_database_filename)
  create_table_sql="""
  CREATE TABLE IF NOT EXISTS ProductCategory(
    ProductCategoryID INTEGER PRIMARY KEY AUTOINCREMENT,
    ProductCategory TEXT NOT NULL,
    ProductCategoryDescription TEXT NOT NULL
  );"""

  create_table(conn, create_table_sql, drop_table_name="ProductCategory")

  with conn:
    conn.executemany(
      "INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (?,?);",
       categories
    ) 
  conn.close()     

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
# WRITE YOUR CODE HERE
  conn=sqlite3.connect(normalized_database_filename)
  cur=conn.cursor()

  cur.execute("SELECT ProductCategoryID, ProductCategory FROM ProductCategory;")
  rows=cur.fetchall()
  conn.close()

  productcategory_to_productcategoryid_dict={}
  for category_id, category_name in rows:
    productcategory_to_productcategoryid_dict[category_name]=category_id

  return productcategory_to_productcategoryid_dict  
        

def step9_create_product_table(data_filename, normalized_database_filename):
  
    # Inputs: Name of the data and normalized database filename
    # Output: None

# WRITE YOUR CODE HERE
  productcategory_to_productcategoryid_dict=step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)

  lines=_read_lines(data_filename)
  products= set()

  for line in lines:
    parts =line.split("\t")
    if len(parts)>=9:
      product_names=parts[5].split(";")
      product_categories=parts[6].split(";")
      product_prices=parts[8].split(";")

      for name, category, price in zip(product_names, product_categories, product_prices):
        name=name.strip()
        category=category.strip()
        try:
          price=float(price.strip())
        except:
          continue

        if name and category and category in productcategory_to_productcategoryid_dict:
          category_id=productcategory_to_productcategoryid_dict[category]
          products.add((name, price, category_id))

  products=sorted(products, key=lambda x:x[0])

  conn=create_connection(normalized_database_filename)
  create_table_sql = """
    CREATE TABLE IF NOT EXISTS Product(
        ProductID INTEGER PRIMARY KEY AUTOINCREMENT,
        ProductName TEXT NOT NULL,
        ProductUnitPrice REAL NOT NULL,
        ProductCategoryID INTEGER NOT NULL,
        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
    );"""


  create_table(conn, create_table_sql, drop_table_name="Product")

    # Insert products
  with conn:
    conn.executemany(
      "INSERT INTO Product (ProductName, ProductUnitPrice, ProductCategoryID) VALUES (?, ?, ?);",
       products
    )

  conn.close()            

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
# WRITE YOUR CODE HERE
  conn=sqlite3.connect(normalized_database_filename)
  cur=conn.cursor()

  cur.execute("SELECT ProductID, ProductName FROM Product;")
  rows=cur.fetchall()
  conn.close()

  product_to_productid_dict={}
  for product_id, product_name in rows:
    product_to_productid_dict[product_name] = product_id

  return product_to_productid_dict        

def step11_create_orderdetail_table(data_filename, normalized_database_filename):
    import sqlite3
    import datetime

    # Get dictionaries
    customer_to_customerid_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    product_to_productid_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)

    # Read lines
    lines = _read_lines(data_filename)
    orders = []

    for line in lines:
        parts = line.split("\t")
        if len(parts) < 11:
            continue

        # Name handling
        name_parts = parts[0].strip().split()
        first_name = name_parts[0].strip()
        last_name = " ".join(name_parts[1:]).strip()
        customer_name = first_name + " " + last_name

        if customer_name not in customer_to_customerid_dict:
            continue
        customer_id = customer_to_customerid_dict[customer_name]

        # Products info
        product_names = parts[5].split(";")
        product_categories = parts[6].split(";")
        quantities = parts[9].split(";")
        order_dates = parts[10].split(";")

        for pname, category, qty, date in zip(product_names, product_categories, quantities, order_dates):
            pname = pname.strip()
            category = category.strip()
            qty = qty.strip()
            date = date.strip()

            if pname not in product_to_productid_dict:
                continue
            product_id = product_to_productid_dict[pname]

            try:
                quantity = int(qty)
                order_date = datetime.datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
            except:
                continue

            orders.append((customer_id, product_id, order_date, quantity))

    # Create table
    conn = create_connection(normalized_database_filename)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS OrderDetail(
        OrderID INTEGER PRIMARY KEY AUTOINCREMENT,
        CustomerID INTEGER NOT NULL,
        ProductID INTEGER NOT NULL,
        OrderDate TEXT NOT NULL,
        QuantityOrdered INTEGER NOT NULL,
        FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
    );"""
    create_table(conn, create_table_sql, drop_table_name="OrderDetail")

    # Insert orders in **exact CSV order**
    with conn:
        conn.executemany(
            "INSERT INTO OrderDetail (CustomerID, ProductID, OrderDate, QuantityOrdered) VALUES (?, ?, ?, ?);",
            orders
        )

    conn.close()


              

def ex1(conn, CustomerName):

    sql_statement = f"""
    SELECT
        c.FirstName || ' ' || c.LastName AS Name,
        p.ProductName,
        o.OrderDate,
        p.ProductUnitPrice,
        o.QuantityOrdered,
        ROUND(p.ProductUnitPrice * o.QuantityOrdered, 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    WHERE c.FirstName || ' ' || c.LastName = '{CustomerName}';
    """

    return sql_statement

def ex2(conn, CustomerName):

    sql_statement = f"""
    SELECT
        c.FirstName || ' ' || c.LastName AS Name,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    WHERE c.FirstName || ' ' || c.LastName = '{CustomerName}'
    GROUP BY o.CustomerID;
    """

    return sql_statement

def ex3(conn):

    sql_statement = """
    SELECT
        c.FirstName || ' ' || c.LastName AS Name,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    GROUP BY o.CustomerID
    ORDER BY Total DESC;
    """

    return sql_statement

def ex4(conn):

    sql_statement = """
    SELECT
        r.Region,
        ROUND(SUM(p.ProductUnitPrice * o.QuantityOrdered), 2) AS Total
    FROM OrderDetail o
    JOIN Customer c ON o.CustomerID = c.CustomerID
    JOIN Product p ON o.ProductID = p.ProductID
    JOIN Country co ON c.CountryID = co.CountryID
    JOIN Region r ON co.RegionID = r.RegionID
    GROUP BY r.Region
    ORDER BY Total DESC;
    """

    return sql_statement

def ex5(conn):

    sql_statement = """
    SELECT
      Y.Country AS Country,
      ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered), 0) AS Total
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product  P ON O.ProductID  = P.ProductID
    JOIN Country Y ON C.CountryID   = Y.CountryID
    GROUP BY Y.CountryID
    ORDER BY Total DESC

    """

    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region

    sql_statement = """
    SELECT 
      R.Region,
      Y.Country,
    ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
    DENSE_RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS TotalRank
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product P ON O.ProductID = P.ProductID
    JOIN Country Y ON C.CountryID = Y.CountryID
    JOIN Region R ON Y.RegionID = R.RegionID
    GROUP BY R.Region, Y.Country
    ORDER BY R.Region ASC, CountryTotal DESC

    """

    return sql_statement

    

# WRITE YOUR CODE HERE
   

def ex7(conn):
    
    # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, Total, TotalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"

    sql_statement = """
    WITH CountryStats AS (
    SELECT 
        R.Region,
        Y.Country,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
        DENSE_RANK() OVER (PARTITION BY R.Region ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS CountryRegionalRank
    FROM OrderDetail O
    JOIN Customer C ON O.CustomerID = C.CustomerID
    JOIN Product P ON O.ProductID = P.ProductID
    JOIN Country Y ON C.CountryID = Y.CountryID
    JOIN Region R ON Y.RegionID = R.RegionID
    GROUP BY R.Region, Y.Country
    )
    SELECT 
        Region,
        Country,
        CountryTotal,
        CountryRegionalRank
    FROM CountryStats
    WHERE CountryRegionalRank = 1
    ORDER BY Region ASC

    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!

    sql_statement = """
    SELECT 
        'Q' || ((CAST(strftime('%m', O.OrderDate) AS INTEGER) + 2) / 3) AS Quarter,
        CAST(strftime('%Y', O.OrderDate) AS INTEGER) AS Year,
        O.CustomerID,
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
    FROM OrderDetail O
    JOIN Product P ON O.ProductID = P.ProductID
    GROUP BY 
        'Q' || ((CAST(strftime('%m', O.OrderDate) AS INTEGER) + 2) / 3),
        CAST(strftime('%Y', O.OrderDate) AS INTEGER),
        O.CustomerID
    ORDER BY Year ASC, Quarter ASC, O.CustomerID ASC;
    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()

    sql_statement = """
    WITH CustomerSales AS (
    SELECT 
        -- Convert month to quarter (Q1–Q4)
        'Q' || ((CAST(strftime('%m', O.OrderDate) AS INT) + 2) / 3) AS Quarter,
        
        -- Extract year
        CAST(strftime('%Y', O.OrderDate) AS INT) AS Year,
        
        O.CustomerID,
        
        -- Total sales per customer per quarter
        ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
    FROM OrderDetail O
    JOIN Product P ON O.ProductID = P.ProductID
    GROUP BY 
        Quarter,
        Year,
        O.CustomerID
),

RankedSales AS (
    SELECT
        Quarter,
        Year,
        CustomerID,
        Total,
        
        -- Rank customers by total sales inside each quarter and year
        DENSE_RANK() OVER (
            PARTITION BY Year, Quarter 
            ORDER BY Total DESC
        ) AS CustomerRank
    FROM CustomerSales
)

-- Final top-5 customers per quarter
SELECT
    Quarter,
    Year,
    CustomerID,
    Total,
    CustomerRank
FROM RankedSales
WHERE CustomerRank <= 5
ORDER BY 
    Year ASC,
    Quarter ASC,
    Total DESC;

    """
# WRITE YOUR CODE HERE
    return sql_statement

def ex10(conn):
    sql = """
        WITH MonthlyTotals AS (
            SELECT
                strftime('%m', O.OrderDate) AS MonthNum,
                SUM(ROUND(P.ProductUnitPrice * O.QuantityOrdered)) AS Total
            FROM OrderDetail O
            JOIN Product P ON O.ProductID = P.ProductID
            GROUP BY MonthNum
        ),
        MonthNames AS (
            SELECT
                CASE MonthNum
                    WHEN '01' THEN 'January'
                    WHEN '02' THEN 'February'
                    WHEN '03' THEN 'March'
                    WHEN '04' THEN 'April'
                    WHEN '05' THEN 'May'
                    WHEN '06' THEN 'June'
                    WHEN '07' THEN 'July'
                    WHEN '08' THEN 'August'
                    WHEN '09' THEN 'September'
                    WHEN '10' THEN 'October'
                    WHEN '11' THEN 'November'
                    WHEN '12' THEN 'December'
                END AS Month,
                Total
            FROM MonthlyTotals
        )
        SELECT
            Month,
            Total * 1.0 AS Total,
            RANK() OVER (ORDER BY Total DESC) AS TotalRank
        FROM MonthNames
        ORDER BY Total DESC;
    """
    return sql



def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag
    sql_statement = """
    WITH OrderedDates AS (
        SELECT
            c.CustomerID,
            c.FirstName,
            c.LastName,
            co.Country,
            od.OrderDate,
            LAG(od.OrderDate) OVER (
                PARTITION BY c.CustomerID
                ORDER BY od.OrderDate
            ) AS PreviousOrderDate
        FROM OrderDetail od
        JOIN Customer c ON c.CustomerID = od.CustomerID
        JOIN Country co ON co.CountryID = c.CountryID
    ),
    Gaps AS (
        SELECT
            CustomerID,
            FirstName,
            LastName,
            Country,
            OrderDate,
            PreviousOrderDate,
            JULIANDAY(OrderDate) - JULIANDAY(PreviousOrderDate) AS DaysWithoutOrder
        FROM OrderedDates
        WHERE PreviousOrderDate IS NOT NULL
    )
    SELECT
        CustomerID,
        FirstName,
        LastName,
        Country,
        OrderDate,
        PreviousOrderDate,
        ROUND(MAX(DaysWithoutOrder), 2) AS MaxDaysWithoutOrder
    FROM Gaps
    GROUP BY CustomerID
    ORDER BY MaxDaysWithoutOrder DESC;
    """
# WRITE YOUR CODE HERE
    return sql_statement
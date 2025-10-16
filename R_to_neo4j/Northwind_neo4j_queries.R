## Task: Query Neo4j database from R using reticulate
## Database: neo4j KG with Northwind sales data
## Autor: Laurence Brandenberger
## Date: October 2025
################################################################################
################################################################################
################################################################################

library(reticulate)

################################################################################
## Set up the connection to the knowlege graph
################################################################################

# import python driver
neo4j <- import("neo4j")

# connect
driver <- neo4j$GraphDatabase$driver(
  "neo4j://127.0.0.1:7687",  
  auth = neo4j$basic_auth("neo4j", "password")
)

# open session
session <- driver$session()

################################################################################
## Define your query and run it
################################################################################

# define query
query <- "
MATCH (n:Employee) 
  RETURN 
    n.employeeID AS employeeID, 
    n.firstName AS employee_firstname,
    n.lastName AS employee_lastname,
    n.title AS employee_title
    ;
"

# run query
res <- session$run(query)
dt <- res$to_df()

################################################################################
## Products: 
################################################################################

# define query
query <- "
MATCH (p:Product)
RETURN p.productName, p.unitPrice
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Counts
################################################################################

# define query
query <- "
MATCH (e:Employee)-[:SOLD]->(o:Order)
RETURN e.firstName, e.lastName, COUNT(o) AS orders
ORDER BY orders DESC
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Products from companies
################################################################################

# define query
query <- "
MATCH (s:Supplier)-[:SUPPLIES]->(p:Product)
RETURN s.companyName, COUNT(p) AS numProducts
ORDER BY numProducts DESC;
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Person selling products 
################################################################################

# define query
query <- "
MATCH (e:Employee)-[:SOLD]->(o:Order)-[:CONTAINS]->(p:Product)
RETURN e.lastName AS Employee, COUNT(DISTINCT p) AS ProductsSold
ORDER BY ProductsSold DESC;
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Price of beverages
################################################################################

# define query
query <- "
MATCH (p:Product)-[:PART_OF]->(c:Category)
WHERE c.categoryName = 'Beverages'
RETURN p.productName, p.unitPrice
ORDER BY p.unitPrice DESC;
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Who sells what the most?
################################################################################

# define query
query <- "
MATCH (e:Employee)-[:SOLD]->(o:Order)-[:CONTAINS]->(p:Product)-[:PART_OF]->(c:Category)
RETURN e.lastName AS Employee, c.categoryName AS Category, COUNT(o) AS NumOrders
ORDER BY NumOrders DESC
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Highest sales by employee => with special returns
################################################################################

# define query: which employee generated the highest total sales by product category
query <- "
MATCH (e:Employee)-[:SOLD]->(o:Order)-[:CONTAINS]->(p:Product)-[:PART_OF]->(c:Category)
WITH e, c, SUM(p.unitPrice) AS totalRevenue, COUNT(DISTINCT o) AS numOrders
WHERE numOrders > 1
RETURN 
    e.firstName + ' ' + e.lastName AS Employee,
    c.categoryName AS Category,
    ROUND(totalRevenue, 2) AS TotalRevenue,
    numOrders
ORDER BY totalRevenue DESC, numOrders DESC
"
# run query
res <- session$run(query)
dt <- res$to_df()


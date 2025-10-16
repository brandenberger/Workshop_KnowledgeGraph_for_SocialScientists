## Task: Query Neo4j database from R using reticulate
## Database: neo4j KG with UK Parliament data
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
## Task 1
################################################################################








################################################################################
################################################################################
################################################################################

################################################################################
## Solution 2: Check number of debates and MPs involved
################################################################################

# define query
query <- "
MATCH (d:Debate)<-[:AUTHORS|SPONSORS]-(p:Person)
RETURN d.title AS Debate, COUNT(DISTINCT p) AS NumParticipants
ORDER BY NumParticipants DESC
"

# run query
res <- session$run(query)
dt <- res$to_df()

################################################################################
## Solution 3: Check number of debates per department
################################################################################

# define query
query <- "
MATCH (d:Debate)-[:ASSIGNED_TO]->(dept:Department)
RETURN dept.name AS Department, COUNT(DISTINCT d) AS NumDebates
ORDER BY NumDebates DESC
"
# run query
res <- session$run(query)
dt <- res$to_df()


################################################################################
## Solution 4: Cross-party collaboration
################################################################################

# define query
query <- "
MATCH (p1:Person)-[:MEMBER_OF]->(party1:Party),
      (p2:Person)-[:MEMBER_OF]->(party2:Party),
      (p1)-[:AUTHORS|SPONSORS]->(d:Debate)<-[:AUTHORS|SPONSORS]-(p2)
WHERE party1 <> party2
RETURN party1.name AS PartyA, party2.name AS PartyB, 
       COUNT(DISTINCT d) AS SharedDebates
ORDER BY SharedDebates DESC
"
# run query
res <- session$run(query)
dt <- res$to_df()

## Bonus: can you get rid of the duplicates?

################################################################################
## Bonus Task: Number of subjects by party
################################################################################

# define query
query <- "
MATCH (party:Party)<-[:MEMBER_OF]-(mp:Person)
      -[:AUTHORS|SPONSORS]->(d:Debate)-[:HAS]->(s:Subject)
RETURN 
    party.name AS Party, 
    s.name AS Topic, 
    COUNT(DISTINCT d) AS Debates
ORDER BY Party, Debates DESC;
"
# run query
res <- session$run(query)
dt <- res$to_df()


# Workshop: From Entities to Edges – A Practical Introduction to Knowledge Graphs

**Instructor:** Laurence Brandenberger, University of Zurich 
**Assistant:** Yaren Durgun, University of Zurich

---

## 📘 Overview

This workshop introduces social scientists to the principles and practice of **Knowledge Graphs (KGs)**. 
KGs are data structures for connecting and querying complex information.

You’ll learn how to:
- Understand what Knowledge Graphs are and why they matter.
- Build a simple Neo4j Knowledge Graph from CSV or Excel data.
- Validate, query, and explore your graph using Cypher.
- Apply these skills to real-world research data.

---

## 🗂️ Repository Structure

```
Workshop_KnowledgeGraph_for_SocialScientists/
│
├── Tutorial1_Simple/             # Example 1: Northwind business dataset
│   ├── example_northwind.ipynb
│
├── Tutorial2_UKParliament/       # Example 2: UK Parliament dataset
│   ├── load_parliament.py        # Main script for data import
│   ├── parliament_modules.py     # Custom wrappers and preprocessors
│   ├── conversion_schema.yaml    # Schema defining entities and relationships
│   ├── debates.xlsx              # Source data
│   └── requirements.txt          # Python dependencies
│
├── Slides_Workshop_Introduction_to_KnowledgeGraphs.pdf  # Workshop slides (PDF)
│
└── README.md                     # This file
```

---

## 🧰 Requirements

- **Python 3.10+**  
- **Neo4j Desktop** (free community edition)  
- Recommended: a virtual environment (`venv`)

Install dependencies:
```bash
rm -rf .venv
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r Tutorial2_UKParliament/requirements.txt
```

---

## 🚀 How to Run the Tutorials

### 1. Northwind Example
A clean, minimal example for understanding how `data2neo` works.

```bash
jupyter notebook Tutorial1_Simple/example_northwind.ipynb
```

### 2. UK Parliament Example
A realistic, messy dataset to practice schema design, wrappers, and validation.

```bash
cd Tutorial2_UKParliament
python load_parliament.py
```

After import, open Neo4j Browser and explore:
```cypher
CALL db.schema.visualization();
```

---

## 🧠 Learning Outcomes

By the end of this workshop, you will be able to:
- Explain the difference between relational and graph-based data.
- Construct a simple knowledge graph using Neo4j and `data2neo`.
- Write Cypher queries to explore entities, relationships, and attributes.
- Identify how KGs can improve research transparency and reproducibility.

---

## 🔗 Resources

- [Neo4j Desktop Download](https://neo4j.com/download/)
- [data2neo Github](https://github.com/jkminder/data2neo)
- [data2neo Documentation](https://data2neo.jkminder.ch)
- [Neo4j Cypher Manual](https://neo4j.com/docs/cypher-manual/current/)

---

## 🧩 License

This material is provided for educational and research purposes under the [MIT License](LICENSE).

---

## 🙌 Acknowledgements

Developed as part of the *DemocraSci Project* at the University of Zurich.  
Special thanks to Julian Minder for the `data2neo` library.

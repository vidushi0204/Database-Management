# DB362 - A Custom Database Management System

## Broad Objective:
The goal of these assignments is to progressively construct **DB362**, an in-memory database management system designed for efficient query execution and data storage. This system integrates fundamental database concepts such as indexing, query optimization, and pipelined execution. Using **Apache Calcite**, an open-source query optimization and execution framework, we aim to build a system that supports complex queries, storage management, and performance optimization techniques such as B+ Tree indexing and rule-based query optimization.

---

## Stage 1: B+ Tree and Index Scan
**Objective**:
To implement B+ Trees for indexing and an efficient access method for querying CSV files. The B+ Tree allows for efficient point insertions, searches, and retrieval of records from disk-based CSV files. Additionally, the assignment integrates this index structure with **Apache Calcite** by implementing an `IndexScan` operator, enabling efficient query evaluation for `SELECT-FROM-WHERE` queries.

---

## Stage 2: Pipelined Query Execution using Apache Calcite
**Objective**:
To implement and optimize query execution in **DB362** using pipelined execution. In this assignment, physical operators such as `PFilter`, `PProject`, `PSort`, `PJoin`, and `PAggregate` are developed to execute relational algebra expressions. This introduces the idea of pipelining the results of one operator into another, reducing the need for intermediate data storage and improving query execution time.

---

## Stage 3: Rule-Based Query Optimization
**Objective**:
To implement a simple rule-based query optimizer in **DB362** using **Apache Calcite**. The rule-based optimizer applies a set of transformations to the query execution plan to improve performance. Specifically, this assignment focuses on merging projection (`PProject`) and filter (`PFilter`) operators into a single operator to reduce the overhead of processing intermediate results, improving the overall query performance.

---

## Tools and Technologies:
- **Apache Calcite**: A framework for SQL parsing, query optimization, and relational algebra transformations.
- **Java 8**: The programming language used for developing the system.
- **Gradle 4.5**: A build automation tool for managing dependencies and building the project.
---

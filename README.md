# Spatio-Textual Similarity Join (Apache Spark)

## Overview

This project implements a **spatio-textual similarity join** using **Apache Spark**.  
Each data record contains both spatial information (coordinates) and textual information (a set of tokens).

The goal is to efficiently identify pairs of objects from two datasets that satisfy:
- a **spatial distance threshold**, and
- a **textual similarity threshold** (e.g. Jaccard similarity),

under a distributed computing environment.

This project focuses on **distributed join design, candidate filtering, and scalable similarity computation**.

---

## Repository Structure

Spatio-Textual-Similarity-Join/
├─ src/
│  └─ spatio_textual_join.py
├─ data/
│  ├─ sample_A.txt
│  └─ sample_B.txt
├─ .gitignore
└─ README.md

---

## Input Data Format

Each input file represents a dataset of spatial-textual objects.

Each line follows this general format:

<id> <x-coordinate> <y-coordinate> <token1,token2,token3,...>

Example:

1 10.5 23.8 data,science,spark  
2 12.1 21.4 bigdata,analytics  

- Coordinates are numeric values
- Textual attributes are represented as comma-separated tokens
- Token order is not significant

---

## Similarity Definition

### Spatial Similarity

Two objects are considered spatially similar if the Euclidean distance between their coordinates is less than or equal to a given threshold.

### Textual Similarity

Textual similarity is measured using **Jaccard similarity**:

J(A, B) = |A ∩ B| / |A ∪ B|

Two objects are considered textually similar if their Jaccard similarity is greater than or equal to the specified threshold.

---

## How It Works

1. Load two spatial-textual datasets into Spark
2. Apply spatial filtering to reduce candidate pairs
3. Compute textual similarity for candidate pairs
4. Output pairs that satisfy both spatial and textual constraints

The implementation is designed to minimise unnecessary comparisons and improve scalability in a distributed environment.

---

## Requirements

- Python 3.9+
- Apache Spark 3.x
- Java 8 or later

Python dependency:
- pyspark

---

## How to Run

Ensure that Apache Spark is installed and that spark-submit is available in your system PATH.

Example command:

spark-submit src/spatio_textual_join.py data/sample_A.txt data/sample_B.txt

Output will be written to a Spark-generated output directory.

---

## Dataset Notes

The original datasets used for this project are not included due to size and licensing constraints.

Small sample input files are provided for:
- Demonstration purposes
- Verifying input format
- Running the program locally

---

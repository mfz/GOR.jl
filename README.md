# GorJulia

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://mfz.github.io/GorJulia.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://mfz.github.io/GorJulia.jl/dev)
[![Build Status](https://github.com/mfz/GorJulia.jl/workflows/CI/badge.svg)](https://github.com/mfz/GorJulia.jl/actions)

Genome ordered streams in Julia

GorJulia is a Julia library to operate on genome ordered streams. 
Elements of genome ordered streams are sorted by chromosome and position, the first two items of each row element. 
Streaming allows operations on data sets that are larger than available memory, and genomic order speeds up relational operations like joins.

GorJulia supports the Tables.jl interface. This means it works with all sources and sinks that conform to the Tables.jl interface, e.g.

- CSV files,
- SQLite3 tables,
- Parquet files,
- and DataFrames,

and are ordered by chromosome and position.

GorJulia allows creation of complex pipelines by joining together operators using the `|>` syntax. 
Each operator is implemented as a Julia iterator that is parameterized on input and output types. 
This allows for easy extension of the library by user-defined operations.


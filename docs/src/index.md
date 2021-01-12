# GOR.jl Documentation

```@contents
```

## Purpose

GOR.jl is a Julia library to operate on genome ordered
streams. Elements of genome ordered streams are sorted by chromosome
and position, the first two items of each row element. Streaming
allows operations on data sets that are larger than available memory,
and genomic order speeds up relational operations like joins.

GOR.jl supports the `Tables.jl` interface. This means it works with
all sources and sinks that conform to the `Tables.jl` interface, e.g.

- CSV files,
- SQLite3 tables,
- Parquet files,
- and DataFrames,

and are ordered by chromosome and position.

GOR.jl allows creation of complex pipelines by joining together
operators using the `|>` syntax. Each operator is implemented as a
Julia iterator that is parameterized on input and output types. This
allows for easy extension of the library by user-defined operations.


## I/O

```@docs
GorFile(path)
write_gor(rows, path)

GorzFile(path)

ParquetFile(path)
write_parquet(rows, path)
```

## Operations on streams

```@docs
verifyorder(rows)
GOR.select(rows, columns::Symbol...)
GOR.filter(rows, predicate)
GOR.rename(rows, args::Pair...)
GOR.mutate(rows, columns::Tuple, func)

GOR.merge(left, right)
GOR.map(rows, func)

GOR.join(left, right)
```

## Grouping

```@docs
groupby(n=0, groupcols = []; aggregates...)
```

Currently the following aggregators are implemented. See
`OnlineStats.jl` for more ideas.

```@docs
Sum(column::Symbol, val = 0.0)
Count
Avg(column::Symbol)
```


## Index

```@index
```
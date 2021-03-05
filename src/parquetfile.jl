
export ParquetFile,
    write_parquet

import Parquet
import Tables: schema, Schema


struct ParquetFileIter{I} <: AbstractGorIter{I}
    path
    rows::I
end


#Base.eltype(::Type{Parquet.RecordCursor{T}}) where {T} = T
Tables.schema(p::Parquet.RecordCursor{T}) where {T} = Tables.Schema(T)


"""
    ParquetFile(path)

Open Parquet file at `path` as genome ordered stream.

This uses implementation in Parquet.jl, which is not very mature yet.
Iterator struct contains state, so best to use 
`data = () -> ParquetFile("data.parquet")`. 
"""
function ParquetFile(path)

    p = Parquet.File(path)
    rows = Parquet.RecordCursor(p)
    ParquetFileIter{typeof(rows)}(path, rows)
    
end

# TODO: change this so iterate creates RecordCursor

"""
    write_parquet(rows, path)
    rows |> write_parquet(path)

Write genome ordered stream `rows` as Parquet file to `path`.
"""
write_parquet(rows, path) = Parquet.write_parquet(path, rows)
write_parquet(path) = rows -> GOR.write_parquet(rows, path)

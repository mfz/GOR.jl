export ArrowFile,
    write_arrow

import Arrow
import Tables: schema, Schema

struct ArrowFileIter{I} <: AbstractGorIter{I}
    path
    rows::I
end 

"""
    ArrowFile(path)

Open Arrow file at `path` as genome ordered stream
"""

function ArrowFile(path)
    p = Arrow.Table(path)
    rows = Tables.rows(p)
    ArrowFileIter{typeof(rows)}(path, rows)
end 

"""
    write_arrow(rows, path; compress = :lz4, dictencode = false)
    rows |> write_arrow(path; compress = :lz4, dictencode = false)

Write genome ordered stream `rows` as Arrow file to `path`
"""

write_arrow(rows, path; compress = :lz4, dictencode = false) = 
    Arrow.write(path, rows; 
        compress = compress, dictencode = dictencode)

write_arrow(path; compress = :lz4, dictencode = false) = 
    rows -> GOR.write_arrow(rows, path; 
        compress = compress, dictencode = dictencode)

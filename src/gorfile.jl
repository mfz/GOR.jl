
# file to support gor and gorz files


export GorFile,
    GorzFile,
    write_gor
    

using CSV
using Tables
using Mmap
using Libz

function infer_column_types_gor(path; delim = "\t", limit = 10_000)
    
    df = CSV.File(path; delim = delim, pool = false, limit = limit)
    (df.names, df.types)
end


function write_schema(schema, path)
    
    fh = open(path, "w")
    for (k,v) in zip(schema...)
	println(fh, "$k\t$v")
    end
    close(fh)

end


function read_schema(path)

    names = Symbol[]
    types = Type[]
    for l in eachline(path)
	name, type = split(chomp(l), "\t")
	push!(names, Symbol(name))
        push!(types, eval(Meta.parse(type)))
    end
    (names, types)

end



struct GorFileIter{I} <: AbstractGorIter{I}
     path
     rows::I
end

"""
    GorFile(path; delim = "\t", limit = 10_000, first = nothing, last = nothing)

Open gor file at `path` as genome ordered stream.

A gor file is a tab-delimited file with header with first two columns
corresponding to Chrom and Pos. The file needs to be sorted by (Chrom, Pos), 
using e.g. unix command `sort -k1,1 -k2,2n`.

# Arguments

- path: path to file
- delim: file delimiter
- limit: number of rows to use for type inference
- first: first coordinate to report (chrom, pos) or `nothing`
- last:  last coordinate to report (chrom, pos) or `nothing`
"""
function GorFile(path; delim = "\t", limit = 10_000, first = nothing, last = nothing)

    names, types = infer_column_types_gor(path; delim = delim, limit = limit)

    fh = open(path)

    header = readline(fh)
    
    firstpos = first === nothing ? position(fh) : gor_seek(fh, first...)[1]
    lastpos = last === nothing ? filesize(fh) : gor_seek(fh, last[1], last[2] + 1)[1]

    buf = Mmap.mmap(fh, Vector{UInt8}, lastpos - firstpos, firstpos)
    
    rows = Tables.namedtupleiterator(CSV.Rows(buf; delim = delim, types = types, header = names))

    GorFileIter{typeof(rows)}(path, rows)

end

Base.show(io::IO, x::GorFileIter{I}) where {I} = print("<GorFile: $(x.path)>")


#
# support sink
#

"""
    write_gor(rows, path)
    rows |> write_gor(path)

Write genome ordered stream `rows` as tab-delimited text file.
"""
write_gor(rows, path) = CSV.write(path, Tables.rows(rows); delim = "\t") 
write_gor(path) = rows -> write_gor(rows, path)



const BUFFERSIZE = 2^16
const INFLATEDSIZE = 2^15

"""
    infer_column_types(path; delim="\t", limit = 10_000)

Infer column types of gorz file at `path` by parsing up to `limit` lines
from start of file.
"""
function infer_column_types_gorz(path; delim="\t", limit = 10_000)
    
    io = IOBuffer()
    fh = open(path)

    buffer = zeros(UInt8, BUFFERSIZE)
    inflated = zeros(UInt8, INFLATEDSIZE)

    header = readline(fh)
    write(io, header, "\n")
    
    lines = 0
    while lines < limit

        # read next gorz block 
        bavail = readuntil!(fh, UInt8('\n'), buffer)
        bavail == 0 && break

        # inflate
        first = findfirst(x -> x == 0x0, buffer) + 1
        last = to8bit!(buffer, first, bavail-1)
        iavail = inflate!(buffer, first, last, inflated)

        # count lines
        for i = 1:iavail
            if inflated[i] == UInt8('\n')
                lines += 1
            end
        end
        
        # save to io
        unsafe_write(io, pointer(inflated), iavail)
        
    end

    seek(io, 0)
    df = CSV.File(io; delim = delim, pool = false, limit = limit)

    (df.names, df.types)
    
end



mutable struct GorzFile{OT}
    fh::IO
    path::String
    buffer::Vector{UInt8}    # buffer file
    bavail::Int64            # buffer[1:bavail] is valid
    inflated::Vector{UInt8}  # inflated version of buffer
    iavail::Int64            # inflated[1:iavail] is valid 
    first                    # first coordinates to report (chrom, pos) or nothing
    last                     # last coordinates to report (chrom, pos) (not included!) or nothing
end

# NOTE: as data is in blocks, and chrom, Pos give start of block,
# we need to start at the last block < (chrom, Pos) in case of gorz file,
# compared to the first block >= (chrom, pos) in case of gor file
#
# use direction = :forward / :backward to indicate intent



"""
    GorzFile(path; limit = 10_000, first = nothing, last = nothing)

Open compressed gor file (.gorz) at `path` as genome ordered stream.

Use up to `limit` number of rows for type inference. 
"""
function GorzFile(path; limit = 10_000, first = nothing, last = nothing)

    names, types = infer_column_types_gorz(path; limit = limit)

    OT = NamedTuple{Tuple(names), Tuple{types...}}

    fh = open(path)
    header = readline(fh)

    # seek into file if `first` is given
    first === nothing || gor_seek(fh, first[1], first[2]; direction = :lt)
    
    GorzFile{OT}(fh,
                 path,
                 zeros(UInt8, BUFFERSIZE),
                 0,
                 zeros(UInt8, INFLATEDSIZE),
                 0,
                 first,
                 last
                 )
end


Base.IteratorEltype(::Type{GorzFile{OT}}) where {OT} = Base.HasEltype()
Base.eltype(::Type{GorzFile{OT}}) where {OT} = OT

Base.IteratorSize(::Type{GorzFile{OT}}) where {OT} = Base.SizeUnknown()

Tables.istable(g::GorzFile{OT}) where {OT} = true
Tables.rowaccess(g::GorzFile{OT}) where {OT} = true
Tables.schema(g::GorzFile{OT}) where {OT} = Tables.Schema(OT)


# state of iterator is (rows, elt_s)
# rows: CSV.Rows iterator
# elt_s: elt and state of rows

function Base.iterate(g::GorzFile{OT}) where {OT}
    iavail = readgorzblock!(g)
    if iavail > 0
        rows = Tables.namedtupleiterator(CSV.Rows(g.inflated[1:iavail];
                                                  delim = "\t",
                                                  header = [OT.parameters[1]...],
                                                  types = [OT.parameters[2].parameters...]))
        elt_s = iterate(rows)
        elt_s === nothing && return nothing
        return iterate(g, (rows, elt_s))
    end
    return nothing
end


function Base.iterate(g::GorzFile{OT}, state) where {OT}
    rows, elt_s = state

    while true

        if elt_s === nothing
            # current g.inflated used up
            # try to get new one
            iavail = readgorzblock!(g)
            if iavail > 0 
                rows = Tables.namedtupleiterator(CSV.Rows(g.inflated[1:iavail];
                                                          delim = "\t",
                                                          header = [OT.parameters[1]...],
                                                          types = [OT.parameters[2].parameters...]))
                elt_s = iterate(rows)
                elt_s === nothing && return nothing
            else
                return nothing
            end
        end

        # beyond last record to report
        g.last !== nothing && (elt_s[1][1], elt_s[1][2]) > g.last && return nothing

        if g.first === nothing || (elt_s[1][1], elt_s[1][2]) >= g.first
            return elt_s[1], (rows, iterate(rows, elt_s[2]))
        end

        elt_s = iterate(rows, elt_s[2])
        
    end
end






###############################################################################
#
# low-level functions for seeking into gor/gorz files
#


"findlast on sub-array"
function findlast_sub(x::Vector{UInt8}, start::Int64, stop::Int64, value::UInt8)
    for idx = stop:-1:start
        x[idx] == value && return idx
    end
    0
end

"findfirst on sub-array"
function findfirst_sub(x::Vector{UInt8}, start::Int64, stop::Int64, value::UInt8)
    for idx = start:stop
        x[idx] == value && return idx
    end
    0
end


"""
get file position and genomic coords of record in pos .. pos + dir closest to pos

buf can be preallocated array 
"""
function gor_get_pos(fh::IOStream, pos::Int64, dir::Int64 = 2^15, buf = Array{UInt8}(undef, abs(dir)))

    max_pos = filesize(fh)

    @assert abs(dir) > 100
    
    if dir > 0
        pos_ = min(max(pos - 1, 0), max_pos )
        seek(fh, pos_)   # try to read 1 char before pos, could be a \n
        n_bytes_read = readbytes!(fh, buf, dir)
        
        if pos_ == 0
            nl = 0
        else
            nl = findfirst_sub(buf, 1, n_bytes_read, UInt8('\n'))

            if nl == 0 || (pos_ + nl == max_pos)
                # not found
                error("No record found within $(pos_ + 1) - $(min(pos_ + dir, max_pos))")
            end
        end
        
        
        if ((nl + 100) > n_bytes_read) && (pos_ + n_bytes_read  < max_pos)
            return gor_get_pos(fh, pos_ + nl, 200, buf)
        end

        chrom, start = split(String(buf[(nl+1):min(nl+100, n_bytes_read)]), '\t'; limit = 3)

        return (pos_ + nl, chrom, parse(Int64, start))
        
    elseif dir < 0
        
        pos_ = min(max(pos + dir, 0), max(max_pos + dir, 0))
        seek(fh, pos_)
        n_bytes_read = readbytes!(fh, buf, -dir)
        nl = findlast_sub(buf, 1, n_bytes_read, UInt8('\n'))

        if pos_ + nl == max_pos
            # '\n' as last character in file
            nl = findlast_sub(buf, 1, n_bytes_read-1, UInt8('\n'))
        end
        
        if nl == 0
            # not found
            error("No record found within $(pos_ + dir) - $(pos_)")
        end

        # want to have at least 100 bytes after \n
        # except we are at end of file
        if ((nl + 100) > n_bytes_read) && (pos_ + n_bytes_read  < max_pos)
            return gor_get_pos(fh, pos_ + nl, 200, buf)
        end
        
        chrom, start = split(String(buf[(nl+1):min(nl + 100, n_bytes_read)]), '\t'; limit = 3)

        return (pos_ + nl, chrom, parse(Int64, start))

    else
        error("dir cannot be 0!")
    end

end


"""
    gor_seek(fh::IOStream, chrom::AbstractString, start::Int64;
                  direction = :ge, max_rec_size = 2^15, header = true)

Seek to first record >= (chrom, start) for direction = :ge,
or last record < (chrom, start) for direction = :lt
in gor file using binary search.

Return (file pos of record, record chrom, record start)
"""
function gor_seek(fh::IOStream, chrom::AbstractString, start::Int64;
                  direction = :ge, max_rec_size = 2^15, header = true, verbose = false)

    buf = Array{UInt8}(undef, 2*max_rec_size)
    
    # find first and last record in file

    low, low_chrom, low_start = gor_get_pos(fh, header*2, max_rec_size, buf)
    high, high_chrom, high_start = gor_get_pos(fh, filesize(fh), -max_rec_size, buf)

    if verbose
        println("first record: $low  $low_chrom  $low_start")
        println("last record:  $high  $high_chrom  $high_start")
    end
    
    @assert (high_chrom, high_start) >= (low_chrom, low_start)

    # done if if first record >= (chrom, start) ...
    if (low_chrom, low_start) >= (chrom, start)
        seek(fh, low)
        return (low, low_chrom, low_start)
    # or last record < (chrom, start)
    elseif (high_chrom, high_start) < (chrom, start)
        seekend(fh)
        return (filesize(fh), "", 0)
    end

    # at this stage [low, high + max_rec_size] contains this record
    # use binary search to decrease width of interval

    verbose && println("Starting binary search")
    
    while low + max_rec_size < high
        mid = low + div(high-low+1, 2)
        _, rchrom, rstart = gor_get_pos(fh, mid, max_rec_size, buf)
        if (rchrom, rstart) >= (chrom, start)
            high = mid
        else
            low = mid
        end

        if verbose
            println("  probing $mid $rchrom  $rstart")
            println("  -> new interval ($low, $high)")
        end
        
    end

    # at this stage, the desired record starts between low and high + max_rec_size
    # and high - low < max_rec_size bytes
    
    seek(fh, low)
    n_bytes_read = readbytes!(fh, buf, 2*max_rec_size)
    i = findnext(x -> x == UInt8('\n'), buf, 1)
    rchrom, rstart = split(String(buf[(i+1):min(i + 101, n_bytes_read)]), '\t'; limit = 3)

    verbose && println("Starting linear search at $(low+i) $rchrom $rstart")

    
    # mark_pos shall mark the last position < (chrom, pos)
    mark_pos = low + i
    mark_chrom = rchrom
    mark_start = parse(Int64, rstart)

    # make sure the first position in (low, high + max_rec_size) is before (chrom, pos)
    @assert (mark_chrom, mark_start) < (chrom, start)
    
    i = i + 1
    
    while i < n_bytes_read
        i = findnext(x -> x == UInt8('\n'), buf, i)
        rchrom, rstart = split(String(buf[(i+1):min(i + 101, n_bytes_read)]), '\t'; limit = 3)

        verbose && println("  found $(low + i)  $rchrom  $rstart")
        
        if (rchrom, parse(Int64, rstart)) < (chrom, start)
            mark_pos = low + i
            mark_chrom = rchrom
            mark_start = parse(Int64, rstart)
        else
            if direction == :ge
                seek(fh, low + i)
                return (low + i, rchrom, parse(Int64, rstart))
            elseif direction == :lt
                seek(fh, mark_pos)
                return (mark_pos, mark_chrom, mark_start)
            else
                error("Invalid direction: $direction")
            end 
        end

        i = i + 1
    end

    error("This should not have happened!")

end


###############################################################################
#
# low-level functions for decompressing gorz files
#


"""
convert printable text back to 8 bit representation
"""
function to8bit!(buffer::Vector{UInt8}, first, last)
    bit = 0
    readPos = first
    writePos = first
    while readPos < last
        b1 = buffer[readPos] - 33
        readPos += 1
        b2 = buffer[readPos] - 33
        tmp = ((b1 & 0xff) >> bit) | ((b2 & 0xff) << (7-bit)) & 0xff
        buffer[writePos] = tmp
        writePos += 1
        bit += 1
        if bit == 7
            bit = 0
            readPos += 1
        end
    end
    writePos - 1 # returns last valid pos in buffer
end


"""
zlib inflate ´source[first:last]´ into ´dest´

return index to last valid entry of ´dest´
"""
function inflate!(source::Vector{UInt8}, first, last, dest::Vector{UInt8})
    strm = Libz.ZStream()
    strm.total_in = last - first + 1
    strm.avail_in = last - first + 1
    strm.total_out = 0
    strm.avail_out = length(dest)
    strm.next_in = pointer(source) + first - 1
    strm.next_out = pointer(dest)

    err = ret = -1

    err = Libz.init_inflate!(strm, windowbits=15+32)
    if (err == Libz.Z_OK)
        err = Libz.inflate!(strm, Libz.Z_FINISH)
        if err == Libz.Z_STREAM_END
            ret = strm.total_out
        else
            Libz.end_inflate!(strm)
            error("Call to Libz.inflate! returned error code $err")
        end
    else
        Libz.end_inflate!(strm)
        error("Call to Libz.init_inflate! returned error code $err")
    end
    
    Libz.end_inflate!(strm)
    ret
end


"""
read from stream s into buffer until delim encountered or buffer full

return number of read bytes
"""
function readuntil!(s::IOStream, delim::UInt8, buffer::Vector{UInt8})
    idx = 0
    n = length(buffer)
    while !eof(s) && idx <= n 
        c = read(s, UInt8)
        idx += 1
        buffer[idx] = c
        c == delim && break
    end
    idx
end


"""
read a gorz block

return number of inflated bytes available
"""
function readgorzblock!(g::GorzFile)
    g.bavail = readuntil!(g.fh, UInt8('\n'), g.buffer)
    if g.bavail == 0
        g.iavail = 0
        return 0
    end
    first = findfirst(x -> x == 0x0, g.buffer) + 1 # first is position after 0x0
    last = to8bit!(g.buffer, first, g.bavail - 1) # convert to 8 bit, ignoring '\n'
    g.iavail = inflate!(g.buffer, first, last, g.inflated)
    return g.iavail
end


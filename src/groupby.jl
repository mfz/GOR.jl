
export groupby

# --------- Group and aggregate --------------------------------
#
# want to support
#
# groupby(iter, n, (:GeneId,); Sum = Sum(:Width), Avg = Avg(:Width))
#
# n = 0 genome
# n > 0 bp in chrom, i.e.  Pos % n
#
# groupby(::iter, ::binsize, ::groupcols; kwargs...)
#
# needs to go through whole ::group, then report stats
#

using DataStructures: DefaultDict

struct GroupBy{I, ET}  
    iter::I
    n::Int64
    groupcols::Vector
    aggregates::Vector{Pair}
end

# output of groupby is
# (Chrom, bpStart, bpEnd, GC1, ..., Agg1, ...)
#
# what to do if aggregate returns struct type? Should we flatten it?

Base.IteratorEltype(::Type{GroupBy{T,ET}}) where {T,ET} = Base.HasEltype()
Base.eltype(::Type{GroupBy{T,ET}}) where {T,ET} = ET
Base.IteratorSize(::Type{GroupBy{T,ET}}) where {T,ET} = Base.SizeUnknown()

Tables.istable(g::GroupBy{T,ET}) where {T,ET} = true
Tables.rowaccess(g::GroupBy{T,ET}) where {T,ET} = true
Tables.schema(g::GroupBy{T,ET}) where {T,ET} = Tables.Schema(ET)


function groupby_(iter, n = 0, groupcols = []; aggregates...)
    # find eltype
    # (Chrom, bpStart, bpEnd, GC1, ..., AggVal1, ...)
    sch = Tables.schema(iter)
    names2types = Dict(zip(sch.names, sch.types))
    names = Symbol[:Chrom, :bpStart, :bpEnd]
    types = [String, Int64, Int64]
    for col in groupcols
        push!(names, col)
        push!(types, names2types[col])
    end
    for (k,v) in aggregates
        push!(names, k)
        push!(types, eltype(v))
    end
    ET = NamedTuple{Tuple(names), Tuple{types...}}
    
    GroupBy{typeof(iter), ET}(iter, n, groupcols, collect(aggregates))
end

"""
    rows |> groupby(n=0, groupcols = []; aggregates...)

Group genome ordered stream by position window and `groupcols`. Summarize groups using `aggregates`.

For each window and combination of grouping columns, compute the online-statistics specified.

# Arguments

- `n::Int` : group by window of size `n`, genomewide for `n=0`
- `groupcols::Vector{Symbol}`: additional grouping columns
- `aggregates` : online statistics to compute 
"""
groupby(n=0, groupcols = []; aggregates...) = rows -> groupby_(rows, n, groupcols; aggregates...)

"""
    genomicbin(g::GroupBy, row)

return genomic bin `(chrom, start, end)` to which `row` belongs
(based on `g.n`)
"""
function genomicbin(g::GroupBy, row)
    if g.n == 0
        bin = ("genome", 0, 3000000000)
    else
        idx = div(row[2], g.n)
        bin = (row[1], idx * g.n, (idx+1)*g.n - 1)
    end
    bin
end

"extract values of g.groupcols from row"
groupkey(g::GroupBy, row) = Tuple([row[col] for col in g.groupcols])


# bin: genomic bin
# groupcols: group columns
# statsdict: Dict{groupkeys -> Dict{Symbol -> Aggregator}}
#
# output [(Chrom, bpStart, bpEnd, <Groupcol1 value>, ..., <Aggregator1 value>, ...)]
function create_aggregator_summary(bin, groupcols, statsdict, ET)
    res = ET[]
    tmp1 = (Chrom = bin[1], bpStart = bin[2], bpEnd = bin[3])
    for (groupkeys, aggdict) in statsdict
        tmp2 = (;zip(groupcols, groupkeys)...)
        tmp3 = (;zip(first.(aggdict), value.(last.(aggdict)))...)
        push!(res, ET((tmp1..., tmp2..., tmp3...)))
    end
    res
end


function advance_group(g::GroupBy{I, ET}, row_iterstate) where {I,ET}

    # determine bin to create statistics for
    bin = genomicbin(g, row_iterstate[1])

    # reset aggregators
    statsdict = DefaultDict(() -> deepcopy(g.aggregates))

    # loop through g.iter, updating aggregators,
    # until next bin or end of strem encountered
    while true
        
        key = groupkey(g, row_iterstate[1])
        fit!(statsdict[key], row_iterstate[1])

        row_iterstate = iterate(g.iter, row_iterstate[2])
        row_iterstate === nothing && break
        
        nextbin = genomicbin(g, row_iterstate[1])
        nextbin != bin && break

    end

    stats = create_aggregator_summary(bin, g.groupcols, statsdict, ET)
    (row_iterstate, stats, 1)

end


function Base.iterate(g::GroupBy)
    row_iterstate = iterate(g.iter)
    row_iterstate === nothing && return nothing
   
    state = row_iterstate, [], 1 
    iterate(g, state)
end


function Base.iterate(g::GroupBy, state)
    row_iterstate, stats, statsidx = state

    while true
        
        if statsidx <= length(stats)
            return stats[statsidx], (row_iterstate, stats, statsidx + 1)
        end

        row_iterstate === nothing && return nothing

        row_iterstate, stats, statsidx = advance_group(g, row_iterstate)
        
    end
end    


    

        

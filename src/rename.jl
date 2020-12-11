
# rename: rename columns

# could just do
# Map(rows, r -> O(r))


export rename

using Tables

struct Rename{I,O}
    rows::I
end

"""
    rename(rows, args::Pair...)
    rows |> rename(args::Pairs...)

Rename columns in genome ordered stream `rows`. 
Old and new column names are specified as `:oldcol => :newcol`.
"""
function rename(rows, args::Pair...)

    sch = Tables.schema(rows)
    incol2outcol = Dict(args...)
    incolumns = sch.names
    outcolumns = Tuple([get(incol2outcol, col, col) for col in incolumns])
    O = NamedTuple{outcolumns, Tuple{sch.types...}}

    Rename{typeof(rows), O}(rows)
end

rename(args::Pair...) = rows -> rename(rows, args...)

Base.IteratorEltype(::Type{Rename{I,O}}) where {I,O} = Base.HasEltype()
Base.eltype(::Type{Rename{I,O}}) where {I,O} = O
Base.IteratorSize(::Type{Rename{I,O}}) where {I,O} = Base.IteratorSize(I)
Base.length(r::Rename{I,O}) where {I,O} = Base.length(r.rows)
Base.size(r::Rename{I,O}) where {I,O} = Base.size(r.rows)

Tables.istable(r::Rename{I,O}) where {I,O} = true
Tables.rowaccess(r::Rename{I,O}) where {I,O} = true
Tables.schema(r::Rename{I,O}) where {I,O} = Tables.Schema(O)

function Base.iterate(r::Rename{I,O}) where {I,O}
    elt_s = iterate(r.rows)
    state = elt_s
    iterate(r, state)
end

function Base.iterate(r::Rename{I,O}, state) where {I,O}
    elt_s = state
    elt_s == nothing && return nothing
    O(elt_s[1]), iterate(r.rows, elt_s[2])
end


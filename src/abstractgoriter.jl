using Tables

abstract type AbstractGorIter{I} end

rows(g::AbstractGorIter) = g.rows

Base.IteratorEltype(::Type{<:AbstractGorIter{I}}) where {I} = Base.IteratorEltype(I)
Base.eltype(::Type{<:AbstractGorIter{I}}) where {I} = Base.eltype(I)
Base.IteratorSize(::Type{<:AbstractGorIter{I}}) where {I} = Base.IteratorSize(I)
Base.length(g::AbstractGorIter) = Base.length(rows(g))
Base.size(g::AbstractGorIter) = Base.size(rows(g))

Base.iterate(g::AbstractGorIter) = iterate(rows(g))
Base.iterate(g::AbstractGorIter, s) = iterate(rows(g), s) 


# implement missing function for Table.jl
Tables.schema(n::Tables.NamedTupleIterator{S,T}) where {S,T} = S()
Tables.schema(::Type{Tables.NamedTupleIterator{S,T}}) where {S,T} = S()


Tables.istable(::AbstractGorIter{I}) where {I} = true
Tables.rowaccess(::AbstractGorIter{I}) where {I} = true
Tables.rows(g::AbstractGorIter{I}) where {I} = rows(g)
Tables.schema(g::AbstractGorIter{I}) where {I} = Tables.schema(rows(g))

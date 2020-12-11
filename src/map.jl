
# map: apply function to elts of iterator
#
# PROBLEM: Type inference in NamedTuples does not work for Union{Missing, T} types

export gormap

struct Map{I,F,M} <: AbstractGorIter{I}
    rows::I
    func::F
end

"""
    gormap(rows, func)
    rows |> gormap(func)

Apply function `func` to elements of genome ordered stream `rows`.
The function `func` should return a `NamedTuple`.

NOTE: Julia cannot infer the type of NamedTuples with `Union{Missing,T}`.
This means that if the input stream `rows` has columns of type `Union{Missing,T}`,
the pipeline probably fails. 
"""
gormap(rows, func) = Map{typeof(rows), typeof(func),
                         Base.return_types(func, (eltype(rows),))[1]}(rows, func)
gormap(func) = rows -> gormap(rows, func)



#transform = gormap

#mutate(rows, func) = gormap(rows, row -> merge(row, func(row)))
#mutate(func) = rows -> mutate(rows, func)


Base.IteratorEltype(::Type{Map{I,F,M}}) where {I,F,M} = Base.HasEltype()
Base.eltype(::Type{Map{I,F,M}}) where {I,F,M} = M
Base.IteratorSize(::Type{Map{I,F,M}}) where {I,F,M} = Base.IteratorSize(I)
Base.length(m::Map{I,F,M}) where {I,F,M} = Base.length(m.rows)
#Base.size(m::Map{I,F,M}) where {I,F,M} = Base.size(m.rows)

Tables.istable(m::Map{I,F,M}) where {I,F,M} = true
Tables.rowaccess(m::Map{I,F,M}) where {I,F,M} = true
Tables.schema(m::Map{I,F,M}) where {I,F,M} = Tables.Schema(M)


function Base.iterate(m::Map{I,F,M}) where {I,F,M}
    elt_s = iterate(m.rows)
    state = elt_s
    iterate(m, state)
end

function Base.iterate(m::Map{I,F,M}, state) where {I,F,M}
    elt_s = state
    elt_s == nothing && return nothing
    M(m.func(elt_s[1])), iterate(m.rows, elt_s[2])
end    

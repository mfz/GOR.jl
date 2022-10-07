
# map: apply function to elts of iterator


export map,
       transform,
       mutate

struct Map{I,F,M} <: AbstractGorIter{I}
    rows::I
    func::F
end

"""
    map(rows, func)
    rows |> map(func)

Apply function `func` to elements of genome ordered stream `rows`.
The function `func` should return a `NamedTuple`.
"""
map(rows, func) = Map{typeof(rows), typeof(func),
                      returntype(func, (eltype(rows),))}(rows, func)
map(func) = rows -> map(rows, func)



transform = map

function mutate(rows, func) 
    @assert Base.return_types(func, (eltype(rows),))[1] <: NamedTuple  "func needs to return a NamedTuple"
    map(rows, row -> Base.merge(row, func(row)))
end
     
mutate(func) = rows -> mutate(rows, func)


Base.IteratorEltype(::Type{Map{I,F,M}}) where {I,F,M} = Base.HasEltype()
Base.eltype(::Type{Map{I,F,M}}) where {I,F,M} = M
Base.IteratorSize(::Type{Map{I,F,M}}) where {I,F,M} = Base.IteratorSize(I)
Base.length(m::Map{I,F,M}) where {I,F,M} = Base.length(m.rows)
#Base.size(m::Map{I,F,M}) where {I,F,M} = Base.size(m.rows)

Tables.istable(m::Map{I,F,M}) where {I,F,M} = true
Tables.rowaccess(m::Map{I,F,M}) where {I,F,M} = true
Tables.schema(m::Map{I,F,M}) where {I,F,M} = Tables.Schema(M)
Tables.rows(m::Map{I,F,M}) where {I,F,M} = collect(m)

function Base.iterate(m::Map{I,F,M}) where {I,F,M}
    elt_s = iterate(m.rows)
    state = elt_s
    iterate(m, state)
end

function Base.iterate(m::Map{I,F,M}, state) where {I,F,M}
    elt_s = state
    elt_s === nothing && return nothing
    M(m.func(elt_s[1])), iterate(m.rows, elt_s[2])
end    

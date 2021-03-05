
# calc(rows, cols, colfunc)
#
# calc((:a,:b), r -> (r.x+2, r.y-3))
# calc(:width, r -> r[3] - r[2] + 1)

export mutate, calc


struct Mutate{I,F,C,O}
    rows::I
    func::F
end

"""
    mutate(rows, cols::Tuple, func)
    mutate(rows, col::Symbol, func)
    rows |> mutate(cols::Tuple, func)
    rows |> mutate(col::Symbol, func)
    
Add or replace columns `cols` computed as `cols = func(row)` to genome ordered stream `rows`.
"""
function mutate(rows, cols::Tuple, func)
    # figure out eltype of merge(r, NamedTuple{cols}(func(r)))
    sch = Tables.schema(rows)
    names = sch.names
    types = sch.types
    name2type = Dict{Symbol, Any}(zip(names, types))
    fnames = cols
    ftypes = Base.return_types(func, (eltype(rows),))[1].parameters
    for (n,t) in zip(fnames, ftypes)
        name2type[n] = t
    end
    onames = (names..., setdiff(fnames, names)...)
    otypes = Tuple((name2type[n] for n in onames))
    O = NamedTuple{onames, Tuple{otypes...}}

    Mutate{typeof(rows), typeof(func), fnames, O}(rows, func)
end

mutate(rows, col::Symbol, func) = mutate(rows, (col,), row -> (func(row),))

mutate(cols::Tuple, func) = rows -> mutate(rows, cols, func)
mutate(col::Symbol, func) = rows -> mutate(rows, col, func)

calc = mutate

Base.IteratorEltype(::Type{Mutate{I,F,C,M}}) where {I,F,C,M} = Base.HasEltype()
Base.eltype(::Type{Mutate{I,F,C,M}}) where {I,F,C,M} = M
Base.IteratorSize(::Type{Mutate{I,F,C,M}}) where {I,F,C,M} = Base.IteratorSize(I)
Base.length(m::Mutate{I,F,C,M}) where {I,F,C,M} = Base.length(m.rows)
#Base.size(m::Mutate{I,F,C,M}) where {I,F,C,M} = Base.size(m.rows)

Tables.istable(m::Mutate{I,F,C,M}) where {I,F,C,M} = true
Tables.rowaccess(m::Mutate{I,F,C,M}) where {I,F,C,M} = true
Tables.schema(m::Mutate{I,F,C,M}) where {I,F,C,M} = Tables.Schema(M)


function Base.iterate(m::Mutate{I,F,C,M}) where {I,F,C,M}
    elt_s = iterate(m.rows)
    state = elt_s
    iterate(m, state)
end

function Base.iterate(m::Mutate{I,F,C,M}, state) where {I,F,C,M}
    elt_s = state
    elt_s == nothing && return nothing
    M(Base.merge(elt_s[1], NamedTuple{C}(m.func(elt_s[1])))), iterate(m.rows, elt_s[2])
end    

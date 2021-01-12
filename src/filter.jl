

export filter

struct Filter{I,F}
    rows::I
    predicate::F
end

"""
    filter(rows, predicate)
    rows |> filter(predicate)

Filter `rows` to include only rows that fulfil `predicate`.
"""
filter(rows, predicate) = Filter{typeof(rows), typeof(predicate)}(rows, predicate)
filter(predicate) = rows -> filter(rows, predicate)

Base.IteratorEltype(::Type{Filter{I,F}}) where {I,F} = Base.HasEltype()
Base.eltype(::Type{Filter{I,F}}) where {I,F} = Base.eltype(I)
Base.IteratorSize(::Type{Filter{I,F}}) where {I,F} = Base.SizeUnknown()

Tables.istable(m::Filter{I,F}) where {I,F} = true
Tables.rowaccess(m::Filter{I,F}) where {I,F} = true
Tables.schema(m::Filter{I,F}) where {I,F} = Tables.Schema(I)

function Base.iterate(m::Filter{I,F}) where {I,F}
    elt_s = iterate(m.rows)
    state = elt_s
    iterate(m, state)
end

function Base.iterate(m::Filter{I,F}, state) where {I,F}
    elt_s = state

    while true
        # done
        elt_s == nothing && return nothing

        # predicate fulfilled
        m.predicate(elt_s[1]) && return elt_s[1], iterate(m.rows, elt_s[2])

        # forward to next row
        elt_s = iterate(m.rows, elt_s[2])        
    end
end    


#
# Iterator to merge left and right stream
#
# output stream has union of columns in left and right
# with types promoted to common type
#


export merge

using Tables

struct Merge{L, R, M} # M is eltype of output
    left::L
    right::R
end

Base.IteratorEltype(::Type{Merge{L,R,M}}) where {L,R,M} = Base.HasEltype()
Base.eltype(::Type{Merge{L,R,M}}) where {L,R,M} = M
Base.IteratorSize(::Type{Merge{L,R,M}}) where {L,R,M} = Base.SizeUnknown()

Tables.istable(m::Merge{L,R,M}) where {L,R,M} = true
Tables.rowaccess(m::Merge{L,R,M}) where {L,R,M} = true
Tables.schema(m::Merge{L,R,M}) where {L,R,M} = Tables.Schema(eltype(Merge{L,R,M}))
Tables.rows(m::Merge{L,R,M}) where {L,R,M} = m 


"""
    merge(left, right)
    left |> merge(right)

Merge `left` and `right` genome ordered streams.

Output stream contains union of columns with type promotion
"""
merge(left, right) = Merge{typeof(left), typeof(right),
                              union_type(eltype(left), eltype(right))}(left, right)
merge(right) = left -> merge(left, right)


function Base.iterate(m::Merge{L,R,M}) where {L,R,M}
    lelt_s = iterate(m.left)
    relt_s = iterate(m.right)
    state = (lelt_s, relt_s)
    iterate(m, state)
end


function Base.iterate(m::Merge{L,R,M}, state) where {L,R,M}
    lelt_s, relt_s = state
    
    while true

        lelt_s == relt_s == nothing && return nothing

        if lelt_s === nothing 
            return fill_named_tuple(relt_s[1], M), (lelt_s, iterate(m.right, relt_s[2]))
        end
        
        if relt_s === nothing
            return fill_named_tuple(lelt_s[1], M), (iterate(m.left, lelt_s[2]), relt_s)
        end

        lpos = (lelt_s[1][1], lelt_s[1][2])
        rpos = (relt_s[1][1], relt_s[1][2])

        if lpos <= rpos
            return fill_named_tuple(lelt_s[1], M), (iterate(m.left, lelt_s[2]), relt_s)
        else
            return fill_named_tuple(relt_s[1], M), (lelt_s, iterate(m.right, relt_s[2]))
        end

    end
end

# allow for the possibility of merging two streams of different types
# here we create the type by taking union of column names,
# and promoting the column types 
function union_type(::Type{NamedTuple{lN,lT}}, ::Type{NamedTuple{rN,rT}}) where {lN, lT, rN, rT}
    names = (lN..., setdiff(rN, lN)...)

    ltypes = Dict(zip(lN, lT.parameters))
    rtypes = Dict(zip(rN, rT.parameters))

    types = (promote_type(get(ltypes, name, Missing),
                          get(rtypes, name, Missing))
             for name in names)

    NamedTuple{names, Tuple{types...}}
end

# convert a NamedTuple to its union type
# 50 times faster than naive function version
@generated function fill_named_tuple(x::NamedTuple{lN, lT}, ::Type{NamedTuple{rN, rT}}) where {lN, lT, rN, rT}

    vals = []
    for n in rN
        if n in lN
            # want to interpolate n as symbol
            # i.e. need to quote it
            push!(vals, :(x[$(Expr(:quote, n))]))
        else
            push!(vals, :(missing))
        end
    end
   
    :(NamedTuple{$rN, $rT}(($(vals...),)))
end

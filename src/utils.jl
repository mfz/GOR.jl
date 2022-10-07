export top,
    skip,
    mergex,
    mergeleftx,
    postfix

using Tables


"""
Take top `n` elements from an iterator
"""
top(n) = rows -> Iterators.take(rows, n)

Tables.istable(t::Iterators.Take{I}) where {I} = true
Tables.rowaccess(t::Iterators.Take{I}) where {I} = true
Tables.schema(t::Iterators.Take{I}) where {I} = Tables.schema(t.xs) #Tables.Schema(eltype(I))



"""
skip `n` elements from an iterator
""" 
skip(n) = rows -> Iterators.drop(rows, n)

Tables.istable(t::Iterators.Drop{I}) where {I} = true
Tables.rowaccess(t::Iterators.Drop{I}) where {I} = true
Tables.schema(t::Iterators.Drop{I}) where {I} = Tables.Schema(eltype(I))


"""
append symbol `s` to `symbols` postfixing it with `postfix` until unique
"""
# should we call it push_uniq!  ?
function mergesymbol(symbols::Vector{Symbol}, s::Symbol; postfix = "x")
    if s in symbols
	mergesymbol(symbols, Symbol(string(s)*postfix))
    else
	push!(symbols, s)
    end
    symbols
end

# merge_uniq better name?
@generated function mergex(x::NamedTuple{lN,lT}, y::NamedTuple{rN,rT}) where {lN,lT,rN,rT}
    N = Symbol[lN...]
    for s in rN
	N = mergesymbol(N, s)
    end
    NT = Tuple(N)
    T = Tuple{lT.parameters..., rT.parameters...}
    :(NamedTuple{$NT,$T}((x..., y...))) 
end


@generated function postfix(x::NamedTuple{N,T}, ::Type{Val{K}}) where {N,T,K}
    n = length(N)
    newN = Vector{Symbol}(undef, n)
    for i = 1:n
	newN[i] = Symbol(string(N[i])*string(K))
    end
    :(NamedTuple{$(Tuple(newN)),$T}((x...,)))
end

postfix(name::String) = rows -> (postfix(x, Val{Symbol(name)}) for x in rows)


# for leftjoin

# like mergex, but convert coltypes of right stream y to Union{Missing, coltype}
# merge_maybe better name?
@generated function mergeleftx(x::NamedTuple{lN,lT}, y::NamedTuple{rN,rT}) where {lN,lT,rN,rT}
    N = Symbol[lN...]
    for s in rN
	N = mergesymbol(N, s)
    end
    NT = Tuple(N)
    T = Tuple{lT.parameters..., (Union{Missing, t} for t in rT.parameters)...}
    :(NamedTuple{$NT,$T}((x..., y...))) 
end

# like mergex, but insert all columns from right stream as missing
@generated function mergeleftx(x::NamedTuple{lN,lT}, ::Type{NamedTuple{rN,rT}}) where {lN,lT,rN,rT}
    N = Symbol[lN...]
    for s in rN
	N = mergesymbol(N, s)
    end
    NT = Tuple(N)
    T = Tuple{lT.parameters..., (Union{Missing, t} for t in rT.parameters)...}
    y = repeat([missing], length(rT.parameters))
    :(NamedTuple{$NT,$T}((x..., $(y...))))    

end


# Julia provides function Base.return_types to provide return types of a function
# given the types of its inputs
#
# But there is a difference when the inputs contain a Union missing type
#
# > Base.return_types(x -> (a=2x,b=x+1), (Union{Missing, Int64},))
# 1-element Vector{Any}:
# NamedTuple{(:a, :b), _A} where _A<:Tuple{Union{Missing, Int64}, Union{Missing, Int64}}
# 
# > Base.return_types(x -> (a=2x,b=x+1), (Int64,))
# 1-element Vector{Any}:
# NamedTuple{(:a, :b), Tuple{Int64, Int64}}
#
# Here we define a function that works in both cases

"""

Infer return type of function func -> NamedTuple given the types of its input arguments
"""
function returntype(func, intype)
    rt = Base.return_types(func, intype)[1]

    @assert rt <: NamedTuple "func needs to return a NamedTuple"

    if typeof(rt) === DataType
        return NamedTuple{rt.parameters[1], rt.parameters[2]}
    elseif typeof(rt) === UnionAll
        return NamedTuple{rt.body.parameters[1], rt.var.ub}
    else
        error("Unknown type: $(typeof(rt))")
    end
end 


# do not export this
# should be called as GOR.pkgpath
pkgpath(paths...) = joinpath(@__DIR__, "..", paths...)

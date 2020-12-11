
# groupby aggregators

#
# Aggregators are structs with fit! and value methods
#
# fit!(aggregator, row) updates aggregator with row
# value(aggregator) returns current value
#

export fit!,
    value,
    Sum,
    Count,
    Avg
    

abstract type AbstractAggregator end




mutable struct Sum{F} <: AbstractAggregator
    valfunc::F
    val::Float64
    Sum(valfunc::F, val = 0.0) where {F <: Function} = new{F}(valfunc, val)
end

Base.eltype(::Type{Sum{F}}) where {F} = Float64

"""
    Sum(column::Symbol, val = 0.0)

Aggregator for sum of `column`. 
"""
function Sum(column::Symbol, val = 0.0)
    valfunc = s -> s[column]
    Sum(valfunc, val)
end



function fit!(s::Sum, x)
    s.val += s.valfunc(x)
end

value(s::Sum) = s.val

"""
    Count()

Aggregator for count.
"""
mutable struct Count <: AbstractAggregator
    val::Int64
    Count() = new(0)
end



Base.eltype(::Type{Count}) = Int64


function fit!(c::Count, x)
    c.val += 1
end

value(c::Count) = c.val



mutable struct Avg{F} <: AbstractAggregator
    keyfunc::F
    total::Float64
    n::Int64
    Avg(keyfunc::F) where {F <: Function} = new{F}(keyfunc, 0.0, 0)
end

Base.eltype(::Type{Avg{F}}) where {F} = Float64

"""
    Avg(column::Symbol)

Aggregator for average of `column`.
"""
function Avg(column::Symbol)
    valfunc = s -> s[column]
    Avg(valfunc)
end

function fit!(a::Avg, x)
    a.total += a.keyfunc(x)
    a.n += 1
end

value(a::Avg) = a.total / a.n


# d is collection of key -> Aggregator pairs
# fit!(d, row) fit!s all aggregators with row
function fit!(d, x) 
    for (k, v) in d
        fit!(v, x)
    end
end

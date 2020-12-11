#
# Iterator to join left and right streams on position
#

#
# uses vector as queue for right iterator
#
# iterator state is
# (lelt_s, relt_s, rq, rqidx)
# where lelt_s is current elt from left and state
#       relt_s is element, state that just did not make it into rq
#       rq is queue of rows from right to be reported withh lelt_s[1]
#       rqidx is index into rq to be reported on next iteration
#

#
# How do we combine elts from left and right stream?
#
# If we use an inner join, can simply use mergex from utils.jl.
# If we use a left join, all columns from right stream can be missing,
# i.e. we need to declare their types as Union{Missing, coltype}
# implemented as mergeleftx(left, right) or mergeleftx(left, R) in utils.jl
#



export gorjoin

using Tables

abstract type AbstractJoin{L,R} end


struct InnerJoin{L,R} <: AbstractJoin{L,R}
    left::L
    right::R
    window::Int64
end

struct LeftJoin{L,R} <: AbstractJoin{L,R}
    left::L
    right::R
    window::Int64
end



Base.IteratorEltype(::Type{<:AbstractJoin{L,R}}) where {L,R} = Base.HasEltype()

Base.eltype(::Type{InnerJoin{L,R}}) where {L,R} =
    Base.return_types(mergex, (eltype(L), eltype(R)))[1]

Base.eltype(::Type{LeftJoin{L,R}}) where {L,R} =
    Base.return_types(mergeleftx, (eltype(L), Type{eltype(R)}))[1]

Base.IteratorSize(::Type{<:AbstractJoin{L,R}}) where {L,R} = Base.SizeUnknown()

Tables.istable(j::AbstractJoin{L,R}) where {L,R} = true
Tables.rowaccess(j::AbstractJoin{L,R}) where {L,R} = true
Tables.schema(j::AbstractJoin{L,R}) where {L,R} = Tables.Schema(eltype(j))
Tables.rows(j::AbstractJoin{L,R}) where {L,R} = j



""" 
joins streams `left` and `right` on (:Chrom, :Pos) allowing
`window` difference in position

leftjoin: should left join be performed
"""
gorjoin(left, right; leftjoin = false, window = 0) =
    leftjoin ?
    LeftJoin{typeof(left), typeof(right)}(left, right, window) :
    InnerJoin{typeof(left), typeof(right)}(left, right, window)

gorjoin(right; leftjoin = false, window = 0) =
    left -> gorjoin(left, right; leftjoin = leftjoin, window = window)

# given the current lelt_s,
# update rq to contain all rows to be reported

function advance_right!(j::AbstractJoin, state)
    lelt_s, relt_s, rq, rqidx = state

    # compute new criteria based on position for rows to be included in rq
    min = (lelt_s[1].Chrom, lelt_s[1].Pos - j.window)
    max = (lelt_s[1].Chrom, lelt_s[1].Pos + j.window)

    # remove elts from front of rq that do not fulfil criteria
    while length(rq) > 0 && (rq[1].Chrom, rq[1].Pos) < min
	popfirst!(rq)
    end

    # add elts from right to rq if they fulfil criteria
    while relt_s ≠ nothing && (relt_s[1].Chrom, relt_s[1].Pos) <= max
	push!(rq, relt_s[1])
	relt_s = iterate(j.right, relt_s[2])
    end    

    # return new state
    return (lelt_s, relt_s, rq, 1)
end


function Base.iterate(j::AbstractJoin)

    # prime state for first iteration
    lelt_s = iterate(j.left)
    relt_s = iterate(j.right)
    
    lelt_s == nothing && return nothing
    
    rq = Vector{eltype(j.right)}()
    
    state = advance_right!(j, (lelt_s, relt_s, rq, 1))

    # and iterate
    iterate(j, state)
end


function Base.iterate(j::InnerJoin{L,R}, state) where {L,R}
    
    lelt_s, relt_s, rq, rqidx = state

    while true

	if rqidx <= length(rq)
	    # could put further join conditions here
	    # if fulfilled, return
	    # else rqidx += 1
	    return mergex(lelt_s[1], rq[rqidx]), (lelt_s, relt_s, rq, rqidx + 1)
	end


	if rqidx > length(rq)
	    lelt_s = iterate(j.left, lelt_s[2])
	    lelt_s == nothing && return nothing
	    lelt_s, relt_s, rq, rqidx = advance_right!(j, (lelt_s, relt_s, rq, rqidx))
	end

    end
end

function Base.iterate(j::LeftJoin{L,R}, state) where {L,R}
    
    lelt_s, relt_s, rq, rqidx = state

    while true

	if length(rq) == 0 && rqidx == 1
	    return mergeleftx(lelt_s[1], eltype(R)), (lelt_s, relt_s, rq, rqidx + 1)
	end

	if rqidx <= length(rq)
	    # could put further join conditions here
	    # if fulfilled, return
	    # else rqidx += 1
	    return mergeleftx(lelt_s[1], rq[rqidx]), (lelt_s, relt_s, rq, rqidx + 1)
	end


	if rqidx > length(rq)
	    lelt_s = iterate(j.left, lelt_s[2])
	    lelt_s == nothing && return nothing
	    lelt_s, relt_s, rq, rqidx = advance_right!(j, (lelt_s, relt_s, rq, rqidx))
	end

    end
end


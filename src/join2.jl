#
# Iterator to join left and right streams on position
#

#
# uses vector as heap for right iterator
#
# iterator state is
# (lelt_s, relt_s, rq, rqidx)
# where lelt_s is current elt from left and state
#       relt_s is element, state that just did not make it into rq
#       rq is heap of rows from right to be reported withh lelt_s[1], ordered by (Chrom, bpEnd)
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
using DataStructures: heappush!, heappop!

abstract type AbstractJoin{L,R} end


struct InnerJoin{L,R} <: AbstractJoin{L,R}
    left::L
    right::R
    window::Int64
    kind::Symbol
    lendcol::Int64
    rendcol::Int64
end

struct LeftJoin{L,R} <: AbstractJoin{L,R}
    left::L
    right::R
    window::Int64
    kind::Symbol
    lendcol::Int64
    rendcol::Int64
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


# depending on kind of join
# the interval to be used for overlap is different
# :snpsnp  (start, start)  (start, start)
# :snpseg  (start, start)  (start, end)
# :segsnp  (start, end)    (start, start)
# :segseg  (start, end)    (start, end)

lendcol = (snpsnp = 2, snpseg = 2, segsnp = 3, segseg = 3)
rendcol = (snpsnp = 2, snpseg = 3, segsnp = 2, segseg = 3)

""" 
    gorjoin(left, right; kind = :snpsnp, leftjoin = false, window = 0)

    left |> gorjoin(right; kind = :snpsnp, leftjoin = false, window = 0)

Join genome ordered streams `left` and `right` on `(elt[1], elt[2])`.

# Arguments

- `leftjoin::Bool`: should left join be performed
- `kind::Symbol`: how should overlap be determined (:snpsnp, :snpseg, :segsnp, :segseg)
- `window::Int`: allow `window` base pairs difference in position   
"""
function gorjoin(left, right; kind = :snpsnp, leftjoin = false, window = 0)
    @assert kind in (:snpsnp, :snpseg, :segsnp, :segseg) "Unknown join kind: $kind"
    
    leftjoin ?
        LeftJoin{typeof(left), typeof(right)}(left, right, window, kind, lendcol[kind], rendcol[kind]) :
        InnerJoin{typeof(left), typeof(right)}(left, right, window, kind, lendcol[kind], rendcol[kind])
end


gorjoin(right; kind = :snpsnp, leftjoin = false, window = 0) =
    left -> gorjoin(left, right; leftjoin = leftjoin, window = window, kind = kind)
    




# given the current lelt_s,
# update rq to contain all rows to be reported (maybe more)

function advance_right!(j::AbstractJoin, state)
    lelt_s, relt_s, rq, rqidx = state

    # compute new criteria based on position of left for rows to be included in rq
    lmin = (lelt_s[1][1], lelt_s[1][2] - j.window)
    lmax = (lelt_s[1][1], lelt_s[1][j.lendcol] + j.window)

    # remove elts from front of rq that end before lmin
    while length(rq) > 0 && (rq[1][1] < lmin)
	heappop!(rq)
    end

    # add elts from right to rq if they overlap (lmin, lmax)
    while relt_s ≠ nothing

        # relt after lelt
        relt_s[1][1] > lelt_s[1][1] && break
        relt_s[1][1] == lelt_s[1][1] && relt_s[1][2] > lelt_s[1][j.lendcol] + j.window && break
        # overlap
        if lelt_s[1][1] == relt_s[1][1] && lelt_s[1][2] - j.window <= relt_s[1][j.rendcol] && relt_s[1][2] <= lelt_s[1][j.lendcol] + j.window 
	    heappush!(rq, ((relt_s[1][1], relt_s[1][j.rendcol]), relt_s[1]))
        end
        
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

    # heap, ordered by (rChrom, rEnd)
    rq = Vector{Tuple{Tuple{String, Int64}, eltype(j.right)}}()
    
    state = advance_right!(j, (lelt_s, relt_s, rq, 1))

    # and iterate
    iterate(j, state)
end


function Base.iterate(j::InnerJoin{L,R}, state) where {L,R}
    
    lelt_s, relt_s, rq, rqidx = state

    while true

	if rqidx <= length(rq)
            # need to test for overlap again
            # it could be that left ends before right starts
	    relt = rq[rqidx][2]
            if lelt_s[1][1] == relt[1] && lelt_s[1][2] - j.window <= relt[j.rendcol] && relt[2] <= lelt_s[1][j.lendcol] + j.window
	        return mergex(lelt_s[1], relt), (lelt_s, relt_s, rq, rqidx + 1)
            else
                rqidx += 1
            end
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
	    relt = rq[rqidx][2]
            if lelt_s[1][1] == relt[1] && lelt_s[1][2] - j.window <= relt[j.rendcol] && relt[2] <= lelt_s[1][j.lendcol] + j.window
	        return mergex(lelt_s[1], relt), (lelt_s, relt_s, rq, rqidx + 1)
            else
                rqidx += 1
            end
	end


	if rqidx > length(rq)
	    lelt_s = iterate(j.left, lelt_s[2])
	    lelt_s == nothing && return nothing
	    lelt_s, relt_s, rq, rqidx = advance_right!(j, (lelt_s, relt_s, rq, rqidx))
	end

    end
end


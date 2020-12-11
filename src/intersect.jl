

#
#   UNFINISHED !!!
#
#


#
# Intersection of elements from left and right stream
#

#
# this could be done with an inner join,
# computation of intersection, and resorting
#
# Here we are trying to solve the problem using heaps for left and
# right stream. Each stream has a (bpEnd-sorted) heap that contains
# all currently open segments of that stream
#
# Whenever we encounter a new segment (on left or right stream),
# we take the (bpEnd-sorted) heap from the other stream,
# pop off any segments that have already ended,
# and then report the intersection of the new segment with all
# remaining elements on the heap.
# i.e. lelt.Chrom, max(lelt.bpStart, relt.bpStart),
#                  min(lelt.bpEnd, relt.bpEnd)
#
#
# Need to keep track of which stream produced new segment,
# (:left or :right) and which idx of other streams heap is
# to be reported next
#

export gorintersect

using Tables
using DataStructures: heappush!, heappop!

struct Intersection{L,R}
    left::L
    right::R
end

Base.IteratorEltype(::Type{Intersection{L,R}}) where {L,R} = Base.HasEltype()

#Base.eltype(::Type{Intersection{L,R}}) where {L,R} =
#    Base.return_types(mergex, (eltype(L), eltype(R)))[1]


Base.IteratorSize(::Type{Intersection{L,R}}) where {L,R} = Base.SizeUnknown()

Tables.istable(i::Intersection{L,R}) where {L,R} = true
Tables.rowaccess(i::Intersection{L,R}) where {L,R} = true
Tables.schema(i::Intersection{L,R}) where {L,R} = Tables.Schema(eltype(i))
Tables.rows(i::Intersection{L,R}) where {L,R} = i

gorintersect(left, right) = Intersection{typeof(left), typeof(right)}(left, right)

gorintersect(right) = left -> gorintersect(left, right)



function Base.iterate(i::Intersection{L,R}) where {L,R}

    lelt_s = iterate(i.left)
    lelt_s == nothing && return nothing
    
    relt_s = iterate(i.right)

    lh = Vector{Tuple{String, Int64}, eltype(i.left)}()
    rh = Vector{Tuple{String, Int64}, eltype(i.right)}()

    state = (lelt_s, lh, relt_s, rh, :left, 1)
    iterate(i, state)
        
end



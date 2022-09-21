
export verifyorder

struct VerifyOrder{I} <: AbstractGorIter{I}
    rows::I
end


"""
    verifyorder(rows)
    rows |> verifyorder

Verify order of genome ordered stream `rows`.

An iterator that checks if its input is ordered by `(elt[1], elt[2])`.
The iterator passes the input rows through, but throws an
`ErrorException` if rows are out of order.  
"""
function verifyorder(rows)
    VerifyOrder{typeof(rows)}(rows)
end


function Base.iterate(v::VerifyOrder)
    elt_s = iterate(v.rows)
    elt_s === nothing && return nothing
    elt, s = elt_s
    return elt, elt_s
end


function Base.iterate(v::VerifyOrder, last_s)
    last, s = last_s
    elt_s = iterate(v.rows, s)
    elt_s === nothing && return nothing
    elt, s = elt_s
    
    if (last.Chrom, last.Pos) > (elt.Chrom, elt.Pos)
        msg = "Out of order: ($(elt.Chrom), $(elt.Pos)) after ($(last.Chrom), $(last.Pos))"
        error(msg)
    end

    return elt, elt_s
end


# select: select particular columns
# df |> select(:Chrom, :Pos, :Val)

# could be simplified by using
# NamedTuple{(columns to select)}(intuple)






export select

using Tables

# I is type of rows iterator
# O is output eltype
#
# having columns to select in type signature allows us to use
# @generated iterate function


struct Select{I,O}
    rows::I
end

"""
    select(rows, columns::Symbol...)
    rows |> select(columns::Symbol...)

Select `columns` from genome ordered stream `rows`. 
"""
function select(rows, columns::Symbol...)
    sch = Tables.schema(rows)
    col2type = Dict(zip(sch.names, sch.types))
    outtypes = [col2type[a] for a in columns]
    O = NamedTuple{Tuple(columns), Tuple{outtypes...}}
    
    Select{typeof(rows), O}(rows)
end

select(columns::Symbol...) = rows -> select(rows, columns...)


Base.IteratorEltype(::Type{Select{I,O}}) where {I,O} = Base.HasEltype()
Base.eltype(::Type{Select{I,O}}) where {I,O} = O
Base.IteratorSize(::Type{Select{I,O}}) where {I,O} = Base.IteratorSize(I)
Base.length(r::Select{I,O}) where {I,O} = Base.length(r.rows)
Base.size(r::Select{I,O}) where {I,O} = Base.size(r.rows)

Tables.istable(r::Select{I,O}) where {I,O} = true
Tables.rowaccess(r::Select{I,O}) where {I,O} = true
Tables.schema(r::Select{I,O}) where {I,O} = Tables.Schema(O)


function Base.iterate(s::Select{I,O}) where {I,O}
    elt_s = iterate(s.rows)
    iterate(s, elt_s)
end

@generated function Base.iterate(s::Select{I,O}, state) where {I,O}
    # want to have
    # O(Tuple([elt[:col1], elt[:col2],...]))
    #
    # splatting only works as argument
    # :($(vals...)) creates error
    # but :($(vals...),)  or :(Tuple($(vals...))) works
    
    vals = []
    for n in O.parameters[1]
        push!(vals, :(elt[$(Meta.quot(n))]))
    end
    
    quote
        elt_s = state
        elt_s == nothing && return nothing
        elt, is = elt_s
        O(($(vals...),)), iterate(s.rows, is)
    end
end
        
    
    
    
    
# should we just do a more powerful select?
#
# @select :x, :y, c = 2*:y, _
#
# where _ is a fill-in for everything
# every symbol mentioned explicitly is excluded from _

#
# use macro first to create function for all specified cols
# then pass names (:x, :y, :c, :_) and  (x, y, c) = func(row)
#

# general strategy to process command
#
# @select :a, b = 23 - :b, _, d = 12
#
# 1. process AST using macro stage
#
#    (:a, :b, :_, :d), row -> (row[:a], 23 - row[:b], 12)
#    names           , func
#
#    macro returns function
#    rows -> pass2(rows, names, func)
#
# 2. type inference
#
#    rsch = Tables.schema(rows)
#    rtypes = rsch.types
#    rnames = rsch.names
#    ftypes = Base.return_types(func, (eltype(rows),))[1]
#    fnames = [x for x in names if x != :(_)]
#    ridx = [i for i = 1:length(rnames) if ~(rnames[i] in names)]
#    _names = rnames[ridx]
#    _types = rtypes[idx]
#
#    onames = []
#    otypes = []
#    idx = 1
#    for n in names
#      if n == :(_)
#        # insert _names, _types
#      else
#        push!(names, n)
#        push!(types, ftypes[i])
#        i += 1
#      end
#    end
#
#    pass2 returns a structure that is parameterized on 
#    input iter type, fnames, onames, otypes
#
# 3. generated function
#
#    fvals = func(row)
#    name2val = onames => row[onames]
#    name2val[fnames] = fvals[fnameidx]
#    

# NOTE: step 2 and 3 are basically mutate in mutate.jl


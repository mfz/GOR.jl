
#
# idea was to use sth like
# transform(rows, func) = (func(row) for row in rows)
# where func creates NamedTuple
#
# BUT: type inference in Julia does not seem to work for NamedTuples
#      for Union{T, Missing}
#
# julia> Base.return_types(x -> (2x,x+1), (Union{Missing, Int64},))
# 1-element Array{Any,1}:
#  Tuple{Union{Missing, Int64},Union{Missing, Int64}}
#
# julia> Base.return_types(x -> (a=2x,b=x+1), (Union{Missing, Int64},))
# 1-element Array{Any,1}:
#  NamedTuple{(:a, :b),_A} where _A<:Tuple
#
# but it works for tuples
#
# see https://github.com/JuliaLang/julia/issues/29970 for some explanations


macro transform(args...)
    :(transform_($(transform_helper(args...)...)))
end

macro merge(args...)
    :(esc(merge_($(transform_helper(args...)...))))
end
    

#
# functions to modify AST
# every symbol :col in expression ex gets replaced by rowsymbol[:col]
#
replace_row_vars(ex, rowsymbol) = ex

# treat QuoteNode as regular quote
replace_row_vars(ex::QuoteNode, rowsymbol) = replace_row_vars(Meta.quot(ex.value), rowsymbol)    

function replace_row_vars(ex::Expr, rowsymbol)
    if ex.head == :quote
        :($rowsymbol[$(Meta.quot(ex.args[1]))])
    else
	Expr(ex.head, map(arg -> replace_row_vars(arg, rowsymbol), ex.args)...)
    end 
end

#
# convert list of expressions 
# [var1 = expr1, var2 = expr2, ...]
# info
# tuple of names (:var1, :var2, ...)
# and function row -> (expr1', expr2', ...)
# where expr' = replace_row_vars(expr, :row)
#
function transform_helper(args...)
    names = Symbol[]
    funcs = :(())
    rowsymbol = gensym()
    
    for arg in args
        @assert arg.head == :(=) "expected Symbol = expr"
        @assert isa(arg.args[1], Symbol) "expected Symbol = expr"

        push!(names, arg.args[1])
        push!(funcs.args, replace_row_vars(arg.args[2], rowsymbol))
    end

    Tuple(names), quote $rowsymbol -> $funcs end

end


function transform_(rows, names, func)

    ET = NamedTuple{names, Base.return_types(func, (eltype(rows),))[1]}
    
    GorJulia.Map{typeof(rows), typeof(func), ET}(rows, func)
end
    
transform_(names, func) = rows -> transform_(rows, names, func)

function merge_(rows, names, func)
    mfunc = row -> Base.merge(row, NamedTuple{names}(func(row)))

    # figure out type we get when doing merge(row, func(row))
    sch = Tables.schema(rows)
    onames = sch.names
    otypes = sch.types
    idxs = [idx for idx = 1:length(onames) if ~(onames[idx] in names)]
    nnames = (onames[idxs]..., names...)
    ntypes = (otypes[idxs]..., Base.return_types(func, (eltype(rows),))[1].parameters...)
    
    ET = NamedTuple{nnames, Tuple{ntypes...}}
    
    GorJulia.Map{typeof(rows), typeof(mfunc), ET}(rows, mfunc)
end

merge_(names, func) = rows -> merge_(rows, names, func)



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






# then @select can be used instead of transform or merge

function select_helper(args...)

    names = Symbol[]
    funcs = :(())
    rowsymbol = gensym()

    for arg in args
        if isa(arg, Expr) and arg.head == :(=)
            @assert isa(arg.args[1], Symbol) "expected Symbol = expr"

        elseif isa(arg, Symbol)
            if arg == :_

            else
                push!(names, arg)
                push!(funcs.args, :(esc($arg)))
            end

        elseif isa(arg, QuoteNode)
            push!(names, arg.value)
            push!(funcs.args, :($rowsymbol[$(arg.value)]))
        end
    end

end

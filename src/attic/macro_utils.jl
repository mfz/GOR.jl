
# transform (x = :x, t = 2 * :x - :y)
#
# gormap(r -> (x = r.x, t = 2 * r.x + r.y))
#
# @transform rows body
# gormap(rows, row -> replace_vars(body))

# mutate begin ...  (x = :x, t = t) end
#
# gormap(r -> merge(r, (row -> ...)(r))) 
#

# allow sequential assignment
#
# i.e. we might have
# begin
#  x = :y + 2
#  x = :x
#  t = 2x + :y
#  (t,)
# end
#

#
# 1. traverse body and replace all quoted symbols with gensyms
#    keeping track of replacements
#

"look up or define variable ´var´ in environment ´vars´"
function lookup(vars, var)
    if !haskey(vars, var)
	vars[var] = gensym()
    end
    vars[var]
end

function replace_vars(ex, vars) end

# default
replace_vars(ex, vars) = ex

# treat QuoteNode as regular quote
replace_vars(ex::QuoteNode, vars) = replace_vars(Meta.quot(ex.value), vars)    

function replace_vars(ex::Expr, vars)
    if ex.head == :quote
	lookup(vars, Meta.quot(ex.args[1]))
    else
	Expr(ex.head, map(arg -> replace_vars(arg, vars), ex.args)...)
    end 
end

#
# 2. create declaration block that assigns all vars from 
#



# then we would first unpack row (but macro does not know which columns exist
#
# _x = row[:x] 
# _y = row[:y]
#
# then execute body with transformed vars
# _x = _y + 2
# x = _x
# _t = 2x + _y
#
#
# then return merge(row, (:x = _x, :y = _y, :t = _t))

# default
replace_vars(ex) = ex

# treat QuoteNode as regular quote
replace_vars(ex::QuoteNode) = replace_vars(Meta.quot(ex.value))    

function replace_vars(ex::Expr)
    if ex.head == :quote
        :(row[$(Meta.quot(ex.args[1]))])
    else
	Expr(ex.head, map(arg -> replace_vars(arg), ex.args)...)
    end 
end

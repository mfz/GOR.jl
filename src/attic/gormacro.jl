
# NOT USED FOR THE TIME BEING
# it's not that much more to type to specify anonymous function

# adapted from DataFramesMeta linq macro

export @gor, gor

# idea of gor macro is to allow functions to process parsed AST
# of its arguments before it is evaluated

macro gor(arg)
    esc(replacefuns(replacechains(arg)))
end

# Snippet from Calculus.jl
struct SymbolParameter{T} end
SymbolParameter(s::Symbol) = SymbolParameter{s}()


#
# runs all functions through dispatch (gor functions below)
#

replacefuns(x) = x  # default for non-expression stuff
function replacefuns(e::Expr)
    for i in 1:length(e.args)
        e.args[i] = replacefuns(e.args[i])
    end
    if e.head == :call && isa(e.args[1], Symbol)
        return gor(SymbolParameter(e.args[1]), e.args[2:end]...)
    else
        return e
    end
end



#
# df |> f(x)
# gets converted to
# f(df, x)
#
# i.e. (:call, :|>, :df, (:call, :f, :x))
# to
# (:call, :f, :df, :x)
#

replacechains(x) = x
function replacechains(e::Expr)
    for i in 1:length(e.args)
        e.args[i] = replacechains(e.args[i])
    end
    if e.head == :call && e.args[1] == :|> && isa(e.args[3], Expr)
        newe = e.args[3]
        insert!(newe.args, 2, e.args[2])
        return newe
    else
        return e
    end
end



#
# function dispatch
#

# only functions that need to process the AST need to have
# an entry here
# the others are dealt with using the default case


# default case
gor(::SymbolParameter{s}, args...) where {s} = Expr(:call, s, args...)



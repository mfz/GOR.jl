# Implementation of iterators

Iteration protocol implemented through two functions

- `ìterate(i)`
- `ìterate(i, s)`

Both functions return `(element, state)` if data available, otherwise
`nothing`.

General strategy to implement iterators

Iterators are implemented as immutable structs. They are parametrized
on (at least) input type and output element type.

```
struct Iter{I,O}
       rows::I
end
```

`iterate(i::Iter)` creates starting state `s`, and immediately calls `iterate(i::Iter, s)`


```
function iterate(i::Iter)
	 # prepare state for first iteration
	 state = ...

	 done? && return nothing
	 
	 iterate(i, state)
end
```

```
function iterate(i::Iter, s)
	 elt_s, ... = s

	 while true
	       # state transitions
	 
	 end
end
```

Also implement applicable methods

- `Base.IteratorEltype(::Type{Iter})`
- `Base.eltype(::Type{Iter})`
- `Base.IteratorSize(::Type{Iter})`
- `Base.length(i::Iter)`
- `Base.size(i::Iter)`


To support the Tables.jl interface, also implement

- `Tables.istable(i::Iter)`
- `Tables.rowaccess(i::Iter)`
- `Tables.schema(i::Iter)`
- `Tables.rows(i::Iter)`
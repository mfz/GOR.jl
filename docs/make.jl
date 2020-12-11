using GorJulia
using Documenter

makedocs(;
    modules=[GorJulia],
    authors="Florian Zink <zink.florian@gmail.com> and contributors",
    repo="https://github.com/mfz/GorJulia.jl/blob/{commit}{path}#L{line}",
    sitename="GorJulia.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://mfz.github.io/GorJulia.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/mfz/GorJulia.jl",
)

using GOR
using Documenter

makedocs(;
    modules=[GOR],
    authors="Florian Zink <zink.florian@gmail.com> and contributors",
    repo="https://github.com/mfz/GOR.jl/blob/{commit}{path}#L{line}",
    sitename="GOR.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://mfz.github.io/GOR.jl",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/mfz/GOR.jl",
)

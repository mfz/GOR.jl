@testset "GroupBy" begin

    using DataFrames
    
    df = DataFrame(Chrom = repeat(["chr1"], 10),
                   Pos = 1:10,
                   Gene = ["a", "b", "c", "a", "b", "c", "a", "b", "c", "d"],
                   Value = 1:10)

    di = Tables.namedtupleiterator(df)

    g = di |> GorJulia.groupby(1, [:Gene]; sum = Sum(:Value), count = Count())
    
    @test g |> eltype == 
        NamedTuple{(:Chrom, :bpStart, :bpEnd, :Gene, :sum, :count),Tuple{String,Int64,Int64,String,Float64,Int64}}

    @test g |> DataFrame |> size == (10, 6)

    @test Tables.istable(g)
    @test Tables.rowaccess(g)
    @test Tables.schema(g) == Tables.Schema{(:Chrom, :bpStart, :bpEnd, :Gene, :sum, :count),
                                           Tuple{String,Int64,Int64,String,Float64,Int64}}()
    
end

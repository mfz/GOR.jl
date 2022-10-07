@testset "Map" begin

    using Tables, DataFrames

    df = Tables.namedtupleiterator(DataFrame(Chrom = repeat(["chr1"], 3), Pos = [1,2,3]))

    m = df |> GOR.map(x -> Base.merge(x, (Value = x.Pos * 2,)))

    @test length(m) == 3
    @test m |> collect |> length == 3
    @test m |> DataFrame |> size == (3, 3)
    @test eltype(m) == NamedTuple{(:Chrom, :Pos, :Value),Tuple{String,Int64,Int64}}

    m = df |> GOR.mutate(x -> (Value = x.Pos * 2,))

    @test length(m) == 3
    @test m |> collect |> length == 3
    @test m |> DataFrame |> size == (3, 3)
    @test eltype(m) == NamedTuple{(:Chrom, :Pos, :Value),Tuple{String,Int64,Int64}}

    
end

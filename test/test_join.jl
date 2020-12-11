
@testset "Join" begin

    left = GorFile("left.gor")
    right = GorFile("right.gor")

    j = gorjoin(left, right)

    @test left |> gorjoin(right) |> collect |> length == 100

    @test eltype(j) == NamedTuple{(:Chrom, :Pos, :Val, :Chromx, :Posx, :Valx),Tuple{String,Int64,String,String,Int64,String}}


    using DataFrames, Tables

    l = Tables.namedtupleiterator(DataFrame(
        Chrom = repeat(["chr1"], 11),
        bpStart = collect(0:10:100),
        bpEnd = collect(10:10:110)))
    
    r = Tables.namedtupleiterator(DataFrame(
        Chrom = ["chr1", "chr1", "chr1"],
        bpStart = [15, 55, 75],
        bpEnd = [45, 85, 130]))

    @test r |> gorjoin(l; leftjoin=false, kind = :segsnp) |>
        DataFrame |> size == (9, 6)

    @test r |> gorjoin(l; leftjoin = false, kind = :segseg) |>
        DataFrame |> size == (12, 6)

    @test l |> gorjoin(r; leftjoin = false, kind = :segseg) |>
        DataFrame |> size == (12, 6)
    
    
    @test l |> gorjoin(r) |> eltype ==
        NamedTuple{(:Chrom, :bpStart, :bpEnd, :Chromx, :bpStartx, :bpEndx),Tuple{String,Int64,Int64,String,Int64,Int64}}

    @test l |> gorjoin(r; leftjoin = true) |> eltype ==
        NamedTuple{(:Chrom, :bpStart, :bpEnd, :Chromx, :bpStartx, :bpEndx),Tuple{String,Int64,Int64,Union{Missing, String},Union{Missing, Int64},Union{Missing, Int64}}}
    
    
end

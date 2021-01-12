
@testset "Merge" begin

    left = GorFile("left.gor")
    right = GorFile("right.gor")

    m = left |> gormerge(right)
    @test m |> collect |> length == 200

    @test eltype(m) == NamedTuple{(:Chrom, :Pos, :Val), Tuple{String, Int64, String}}

    using DataFrames

    df1 = Tables.namedtupleiterator(DataFrame(Chrom = [1,2,3], Pos = [1,2,3],
                                              val1 = [1,2,3]))
    df2 = Tables.namedtupleiterator(DataFrame(Chrom = [1,2,3], Pos = [1,2,3],
                                              val1 = [1.0, 2, missing],
                                              val2 = [1,2,3]))
    
    m2 = df1 |> GOR.merge(df2)

    @test eltype(m2) == NamedTuple{(:Chrom, :Pos, :val1, :val2),Tuple{Int64,Int64,Union{Missing, Float64},Union{Missing, Int64}}}
                                  

    @test m2 |> collect |> length == 6

    
    using Tables
    
    @test Tables.istable(m)
    @test Tables.rowaccess(m)
    @test Tables.schema(m).names == (:Chrom, :Pos, :Val)
    @test Tables.schema(m).types == (String, Int64, String)
    @test Tables.schema(m) == Tables.Schema{(:Chrom, :Pos, :Val), Tuple{String, Int64, String}}()

end




@testset "ParquetFile" begin

    @testset "ParquetFile" begin
    
        left = ParquetFile("left.parquet")

        @test eltype(left) == NamedTuple{(:Chrom, :Pos, :Val),Tuple{Union{Missing,String},Union{Missing,Int64},Union{Missing,String}}}

        val, state = iterate(left)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")
    end

    @testset "Table interface" begin

        using Tables
        left = ParquetFile("left.parquet")
        
        @test Tables.istable(left)
        @test Tables.rowaccess(left)
        @test Tables.schema(left) == Tables.Schema{(:Chrom, :Pos, :Val),Tuple{Union{Missing,String},Union{Missing,Int64},Union{Missing,String}}}()
        
        ri = Tables.rows(left)
        val, state = iterate(ri)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")

    end
end

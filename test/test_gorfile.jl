


@testset "GorFile" begin

    @testset "GorFile" begin
    
        left = GorFile(GorJulia.pkgpath("test", "left.gor"))

        @test eltype(left) == NamedTuple{(:Chrom, :Pos, :Val),Tuple{String,Int64,String}}

        val, state = iterate(left)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")
    end

    @testset "Table interface" begin

        using Tables
        left = GorFile(GorJulia.pkgpath("test", "left.gor"))
        
        @test Tables.istable(left)
        @test Tables.rowaccess(left)
        @test Tables.schema(left) == Tables.Schema{(:Chrom, :Pos, :Val),Tuple{String,Int64,String}}()
        
        ri = Tables.rows(left)
        val, state = iterate(ri)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")

    end
end

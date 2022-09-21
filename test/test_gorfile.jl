
import InlineStrings

@testset "GorFile" begin

    @testset "GorFile" begin
    
        left = GorFile(GOR.pkgpath("test", "left.gor"))

        @test eltype(left) == NamedTuple{(:Chrom, :Pos, :Val),Tuple{InlineStrings.String7,Int64,InlineStrings.String7}}

        val, state = iterate(left)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")
    end

    @testset "Table interface" begin

        using Tables
        left = GorFile(GOR.pkgpath("test", "left.gor"))
        
        @test Tables.istable(left)
        @test Tables.rowaccess(left)
        @test Tables.schema(left) == Tables.Schema{(:Chrom, :Pos, :Val),Tuple{InlineStrings.String7,Int64,InlineStrings.String7}}()
        
        ri = Tables.rows(left)
        val, state = iterate(ri)
        @test val == (Chrom = "chr1", Pos = 1, Val = "l1")

    end
end

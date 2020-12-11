
@testset "VerifyOrder" begin

    using DataFrames
    using Tables

    @test Tables.rows(DataFrame(Chrom = ["chr1", "chr1", "chr1"],
                                Pos = [1,2,3])) |> verifyorder |> collect |> length == 3

    @test_throws ErrorException  Tables.rows(DataFrame(Chrom = ["chr1", "chr1", "chr1"],
                                                       Pos = [1,3,2])) |> verifyorder |> collect

    @test_throws ErrorException  Tables.rows(DataFrame(Chrom = ["chr1", "chr2", "chr1"],
                                                       Pos = [1,1,1])) |> verifyorder |> collect

end

@testset "Aggregates" begin

    d = [Dict(:a => 2),
         Dict(:a => 4),
         Dict(:a => 6)]

    s = Dict(:sum => Sum(:a), :count => Count(), :avg => Avg(:a))

    for x in d
        fit!(s, x)
    end
 
    @test value(s[:avg]) == 4.0
    @test value(s[:count]) == 3
    @test value(s[:sum]) == 12.0
    

end

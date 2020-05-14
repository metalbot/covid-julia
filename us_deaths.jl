#
# MIT license
# Copyright 2020 
#
 
using DataFrames
using CSV
using StatsPlots
using Query
using Dates
using TimeSeries
using Statistics
 
#
# initial setup - should really be in a helper function
#
url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_deaths_usafacts.csv"
df = CSV.read(download(url))
dates = Dates.Date.(string.(names(df)[5:end]), "m/d/yy") .+ Dates.Year(2000)
DataFrames.rename!(df, Symbol("County Name") => :County)
states = unique(df[:, :State])
df
today = Dates.format(Dates.today(), "yyyy-mm-dd")
 
 
# filter the 'df' dataframe by states/counties
#
# Inputs -
# - df - a dataframe from above, containing entire US dataset
# - states - a list of state abbrevs. eg ["WA", "OR"] - leave blank for USA as a whole
# - optional list of counties, eg. dofilter(df, ["WA"], ["King County", "Kitsap County"])
#
function dofilter(df, states=[], counties=[])
    x = df |> @filter(_.State in states)
    if length(counties) > 0
        x = x |> @filter(_.County in counties)
    end
    x |> DataFrame
end
 
# return a chart
# inputs:
# - x - filtered dataframe (eg output of dofilter above)
# - title - string title for the chart
# - labels - boolean whether to draw labels or not
function dochart(x, title, labels=false)
    y = [sum(col) for col in eachcol(x[:, 5:end])]
    z = vcat([0], diff(y))
    deaths = DataFrame([dates y])
    DataFrames.rename!(deaths, [:Date, :Deaths])
   
    ta = TimeArray(deaths, timestamp = :Date)
    dta = diff(ta, padding=true)
    mdta = DataFrame(dta)
    DataFrames.rename!(mdta, [:Date, :Increase])
 
    mta = moving(mean, dta, 7)
    mmta = DataFrame(mta)
    DataFrames.rename!(mmta, [:Date, :MovingAvg])
   
    if labels
        l = @layout [a b]
        p1 = @df deaths plot(:Date, :Deaths, formatter = :plain, legend = :topleft, lab="cumulative deaths", ylab="total deaths")
        p2 = @df mdta scatter(:Date, :Increase,title=title,
        ylab="deaths per day", lab="deaths per day", legend = :topleft
    )
        @df mmta plot!(:Date, :MovingAvg, linewidth=3, lab="7 day MA")
        p = plot(p1, p2, layout = l)
    else
        p = @df mdta scatter(:Date, :Increase, legend = false,title=title, color = :red, markersize = 2)
        @df mmta plot!(:Date, :MovingAvg, linewidth=2, color = :blue)
    end
    p
end
 
# helper functions to calculate moving average,
# then attempt to calculate latest trend in moving average
function ma(df)
    y = [sum(col) for col in eachcol(df[:, 5:end])]
    z = vcat([0], diff(y))
    deaths = DataFrame([dates y])
    DataFrames.rename!(deaths, [:Date, :Deaths])
   
    ta = TimeArray(deaths, timestamp = :Date)
    dta = diff(ta, padding=true)
 
    mta = moving(mean, dta, 7)
    mmta = DataFrame(mta)
    DataFrames.rename!(mmta, [:Date, :MovingAvg])
   
    mmta
end
function corm(df)
    madf = ma(df)[2:end, :]
    l = size(madf)[1]
    map(i -> cor(i-9:i, madf[i-9:i, :MovingAvg]), l-20:l)
end
function corm_state(df, state)
    state_data = dofilter(df, [state])
    corm(state_data)
end
function corm_states(df)
    cormdf = DataFrame()
    for s in states
        cormdf[!, Symbol(s)] = corm_state(df, s)
    end
    cormdf
end
 
###################################################################
#                                                                 #
# These are the actual functions that will draw the useful charts #
#                                                                 #
###################################################################
 
#
# Draw the big-ass, all states, graphic.
#
function all_states_graphic(output_to_file = true)
    gr(size=(1200,2400))
    pps = map(s -> dochart(dofilter(df, [s]),s, false),states)
    p = plot(pps..., layout = (17, 3))
    if output_to_file
        png("$today-all-states")
    end
    p
end
 
#
# side-by-side labeled, USA-as-a-whole grpahic
#
function USA(output_to_file=true)
    gr(size=(1024, 320))
    p = dochart(df, "US", true)
    if output_to_file
        png("$today-USA")
    end
    p
end
 
#
# Draw one state, defaulting to WA
#
function one_state(state="WA", output_to_file=true)
    gr(size=(1024,320))
    p = dochart(dofilter(df, [state]), state, true)
    if output_to_file
        png("$today-$state")
    end
    p
end
 
#
# Draw (combined) multiple states, defaulting to WA + OR
#
function multiple_states(states=["OR", "WA"], output_to_file=true)
    gr(size=(1024,320))
    p = dochart(dofilter(df, states), join(states, "/"), true)
    if output_to_file
        png("$today-" * join(states, "-"))
    end
end
 
#
# Draw (combined) data from multiple coutines in one state
#
function counties(state="WA", counties=["King County", "Snohomish County"], output_to_file=true)
    gr(size=(1024,320))
    p = dochart(dofilter(df, [state], counties), state * " - " * join(counties, "/"), true)
    if output_to_file
        png("$today-" * state * "-" * join(counties, "-"))
    end
end
 
#
# Draw the meta-trend, counts of states that are getting better/worse
# Not sure about the math behind this one.
#
function draw_meta_trend(output_to_file=true, cutoffs=[-0.5, 0.5], addendum="mildly")
    corms = corm_states(df)
    worsening = map(row -> length(row |> @filter(_ > cutoffs[2]) |> collect), eachrow(corms))
    improving = map(row -> length(row |> @filter(_ < cutoffs[1]) |> collect), eachrow(corms))
 
    gr(size=(1024,320))
    p = plot(-length(worsening):1:-1, worsening, lab="worsening", xlab="days in past", linewidth=2, color=:red,
    ylab="Number of States", title="Number of States with $addendum improving or worsening 7-day deaths trends")
    plot!(-length(improving):1:-1, improving, lab="improving", linewidth=2, color= :blue)
    if output_to_file
        png("$today-trend-meta-$addendum")
    end
    p
end
 
# Sample calls to generate output, by default outputing to file.
# Feel free to replace these function calls with whatever
# Running this inside jupyter-notebook is recommended when playing with it.
#
# big-ass all-states graphic
all_states_graphic(true)
 
# USA-as-a-whole graphic
USA(true)
 
# Just WA
one_state("WA", true)
 
# Combined data of CA/NV/AZ
multiple_states(["CA", "NV", "AZ"], true)
 
# Just King County in WA
counties("WA", ["King County"], true)
 
# Draw the 'at-least-mildly' improving/worsening chart
draw_meta_trend(true)
 
# Draw the signficantly improving/worsening chart
draw_meta_trend(true, [-0.75, 0.75], "significantly")


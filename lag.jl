
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
 
deaths_url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_deaths_usafacts.csv"
cases_url = "https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv"
 
#
# wrapper struct for downloaded data from one source
# and associated list of dates and states
#
mutable struct DataSource
    df     :: DataFrame
    dates  :: Array{Dates.Date}
    states :: Array{String}
end
 
#
# wrapper struct for all downloaded data
#
mutable struct InputData
    today :: String
    deaths_ds :: DataSource
    cases_ds :: DataSource
end
 
#
# download data from url, extract only data from March 1st onwards, instead
# of the original mid-January (because it's all just zero)
#
function get_data(url) :: DataSource
    df = CSV.read(download(url))
    dates = Dates.Date.(string.(names(df)[44:end]), "m/d/yy") .+ Dates.Year(2000)
    DataFrames.rename!(df, Symbol("County Name") => :County)
    states = unique(df[:, :State])
    # columns 1 to 5 are the headers (state/county/etc)
    # columns 44 onwards are data from March 1st onwards
    # columns 5 through 43 are mid-Jan through Feb 29 (not interesting)
    DataSource(select(df, Not(5:43)), dates, states)
end
 
 
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
 
###################################################################
#                                                                 #
# These are the actual functions that will calculate useful data  #
#                                                                 #
###################################################################
 
 
# helper to generate moving avg from a datasource
function generate_moving_avg(datasource)
    y = [sum(col) for col in eachcol(datasource.df[:, 5:end])]
    z = vcat([0], diff(y))
    raw_data = DataFrame([datasource.dates y])
    DataFrames.rename!(raw_data, [:Date, :Results])
   
    ta = TimeArray(raw_data, timestamp = :Date)
    dta = diff(ta, padding=true)
    mta = moving(mean, dta, 7)
    mmta = DataFrame(mta)
    DataFrames.rename!(mmta, [:Date, :MovingAvg])
    mmta
end
 
#
# Helper to do initial setup
# *** Call this first ***
#
function prep_offset()
    today = Dates.format(Dates.today(), "yyyy-mm-dd")
    deaths_ds = get_data(deaths_url)
    cases_ds = get_data(cases_url)
   
    InputData(today, deaths_ds, cases_ds)
end
 
# Scale the 'cases' moving average to be in the same
# range as the 'deaths' moving average
function generate_scaled_averages(deaths_ds, cases_ds)
    deaths_avg = generate_moving_avg(deaths_ds)
    cases_avg = generate_moving_avg(cases_ds)
    max_d = maximum(deaths_avg[2:end, :MovingAvg])
    max_c = maximum(cases_avg[2:end, :MovingAvg])
    cases_avg[!, :ScaledMovingAvg] = cases_avg[:, :MovingAvg] * (max_d / max_c)
 
    deaths_avg, cases_avg
end
 
# Plot both moving averages, inputs can be left blank for USA-level data
# or a state (or combo of states) for a subset.
# Will be called once-per-state by the 'all_states_graphic' function.
# ('legend=false' can be used to hide the legend for small charts).
function plot_offset(input_data :: InputData, states=[], legend = :topleft)
    if length(states) > 0
        deaths_ds = DataSource(dofilter(input_data.deaths_ds.df, states),
            input_data.deaths_ds.dates, input_data.deaths_ds.states)
        cases_ds = DataSource(dofilter(input_data.cases_ds.df, states),
            input_data.deaths_ds.dates, input_data.deaths_ds.states)
        title = join(states, "/")
    else
        deaths_ds = input_data.deaths_ds
        cases_ds = input_data.cases_ds
        title = "USA"
    end
   
    deaths_avg, cases_avg = generate_scaled_averages(deaths_ds, cases_ds)
    @df deaths_avg plot(:Date, :MovingAvg,
        linewidth=2, color = :red, lab="deaths - 7 day MA",
    legend = legend, title = title)
    @df cases_avg plot!(:Date, :ScaledMovingAvg, linewidth=2, color = :blue, lab="cases - 7 day MA (scaled)")
end
 
# Draw the big-ass all-states chart.
function all_states_graphic(id::InputData, states::Array{String}, output_to_file=false)
    gr(size=(1200,2400))
    pps = map(s -> plot_offset(id, [s], false), states)
    p = plot(pps..., layout = (17, 3))
    if output_to_file
        png("$(id.today)-all-states-lag")
    end
    p
end
 
# do initial set up
id = prep_offset()
 
 
# example - plot Arkansas
gr(size=(720,360))
plot_offset(id, ["AR"])
# output to .png file - skip this next line if you're
# running inside jupyter-notebook and want to see output
png("$(id.today)-AR-lag")
 
# example - plot all states in one big-ass chart
all_states_graphic(id, id.deaths_ds.states, true)
 
 
# example - USA-as-a-whole
gr(size=(720,360))
plot_offset(id)
png("$(id.today)-USA-lag")


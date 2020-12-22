module vobs

using Base
import Dates
import DataFrames
import SQLite

include("arg_handler.jl")
include("vobs_to_sqlite.jl")

import .arg_handler
import .vobs_to_sqlite


function __init__()
   
end


function main(args)
    cmd_message = arg_handler.main_args(args)
    cmd_message[1]==="vobs_to_sqlite" ? vobs_to_sqlite.make_sqlite(cmd_message) : nothing
end


end

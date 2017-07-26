%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月07日12:10:04
%%%-------------------------------------------------------------------
-module(balance_task_app).

-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
-behaviour(application).

%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) ->
    application:ensure_started(eredis_pool),
    application:ensure_started(node_alive),
    application:ensure_started(undead_global),
    global_balance:global_start(),
    balance_task_sup:start_link().

stop(_State) ->
    ok.

%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(task_test).

-export([start_link/1]).

start_link(_) ->
    {ok, spawn_link(fun() -> receive _ -> ok end end)}.

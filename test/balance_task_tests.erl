-module(balance_task_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> application:start(balance_task) end).
-define(Clearnup, fun(_) -> application:stop(balance_task) end).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"redis",
         fun() ->
                 ?assertEqual(ok, element(1, hd(eredis_cluster:qa([<<"INFO">>]))))
         end},
      {"task",
         fun() ->
                 ?assertEqual(ok, balance_task:add_task(<<"test1">>)),
                 ?assertEqual(ok, balance_task:add_task(<<"test2">>)),
                 ?assertEqual(2, length(global_balance:get_tasks())),
                 timer:sleep(1500),
                 ?assertEqual(2, length(balance_task:get_tasks())),
                 ?assertEqual(ok, balance_task:del_task(<<"test1">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test2">>)),
                 ?assertEqual(0, length(global_balance:get_tasks())),
                 timer:sleep(1000),
                 ?assertEqual(0, length(balance_task:get_tasks()))
         end},
      {"node",
         fun() ->
                 ?assertEqual(ok, balance_task:add_task(<<"test1">>)),
                 ?assertEqual(ok, balance_task:add_task(<<"test2">>)),
                 Now = erlang:system_time(seconds),
                 DeadTime = integer_to_binary(Now - 25),
                 {ok, _} = eredis_cluster:transaction([[<<"ZADD">>, <<"${node_alive}_heartbeat_test">>, Now, <<"101">>],
                                                       [<<"INCR">>, <<"${node_alive}_ref_test">>]]),
                 timer:sleep(1000),
                 ?assertEqual(1, length(balance_task:get_tasks())),
                 {ok, _} = eredis_cluster:q([<<"ZADD">>, <<"${node_alive}_heartbeat_test">>, DeadTime, <<"101">>]),
                 timer:sleep(2000),
                 ?assertNotEqual(undefined, balance_task:where_task(<<"test1">>)),
                 ?assertEqual(2, length(balance_task:get_tasks())),
                 ?assertEqual(ok, balance_task:syn_task([<<"test3">>, <<"test4">>, <<"test5">>])),
                 timer:sleep(1000),
                 ?assertEqual(3, length(balance_task:get_tasks()))
         end},
      {"clean",
         fun() ->
                 ?assertEqual(ok, balance_task:del_task(<<"test1">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test2">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test3">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test4">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test5">>)),
                 {ok, _} = eredis_cluster:q([<<"DEL">>, <<"${node_alive}_heartbeat_test">>])
         end}
      ]}
    }.


-module(balance_task_tests).

-include_lib("eunit/include/eunit.hrl").

-define(Setup, fun() -> application:start(balance_task) end).
-define(Clearnup, fun(_) -> application:stop(balance_task) end).

basic_test_() ->
    {inorder,
     {setup, ?Setup, ?Clearnup,
      [{"redis",
         fun() ->
                 ?assertEqual(ok, element(1, eredis_pool:q([<<"INFO">>])))
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
                 {ok, _} = eredis_pool:transaction([[<<"ZADD">>, <<"$node_alive_heartbeat_test">>, Now, <<"101">>],
                                                    [<<"INCR">>, <<"$node_alive_ref_test">>]]),
                 timer:sleep(1000),
                 ?assertEqual(1, length(balance_task:get_tasks())),
                 {ok, _} = eredis_pool:q([<<"ZADD">>, <<"$node_alive_heartbeat_test">>, DeadTime, <<"101">>]),
                 timer:sleep(2000),
                 ?assertEqual(2, length(balance_task:get_tasks())),
                 ?assertEqual(ok, balance_task:del_task(<<"test1">>)),
                 ?assertEqual(ok, balance_task:del_task(<<"test2">>))
         end}
      ]}
    }.


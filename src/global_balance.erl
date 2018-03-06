%%%-----------------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-----------------------------------------------------------------------------
-module(global_balance).

-export([global_start/0, start_link/0]).

-export([get_tasks/0,
         get_redis_tasks/0,
         do_fill_form/6]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-define(REDIS_ALL_TASK(T),      iolist_to_binary([<<"${balance_task}#all_task_">>, T])).
-define(REDIS_NODE_TASK(T, N),  iolist_to_binary([<<"${balance_task}#node_task_">>, T, <<"_">>, N])).
-define(REDIS_TASK_REF(T),      iolist_to_binary([<<"${balance_task}#task_ref_">>, T])).

-define(BALANCE_MODE_AVG, average). %% 平均模式 节点变化重新分配 任务变化全部重新分配
-define(BALANCE_MODE_STD, steady).  %% 平稳模式 节点变化重新分配 删除不会分配 扫射添加
-define(BALANCE_MODE_FXD, fixed).   %% 固定模式 节点变化不重新分配 删除不会分配 扫射添加

-record(state, {tasks = [],
                cache_tasks = [],
                cache_nodes = [],
                way = ?BALANCE_MODE_AVG,
                need_balance = false,
                node = null, live_ref = null}).

%%------------------------------------------------------------------------------
%% @doc interface
%%------------------------------------------------------------------------------
global_start() ->
    ok = application:ensure_started(undead_global),
    ok = undead_global:reg(?MODULE, {?MODULE, start_link, []}).

get_tasks() ->
    State = sys:get_state(?MODULE), State#state.tasks.

get_redis_tasks() ->
    {ok, NodeType} = application:get_env(node_alive, node_type),
    {ok, Tasks} = eredis_cluster:q([<<"SMEMBERS">>, ?REDIS_ALL_TASK(to_binary(NodeType))]), Tasks.

to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X.

%%------------------------------------------------------------------------------
%% @doc gen_server
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, Ref} = node_alive:get_ref(),
    {ok, Type} = application:get_env(node_alive, node_type),
    Way = application:get_env(balance_task, balance_mode, ?BALANCE_MODE_AVG),
    {ok, Tasks} = eredis_cluster:q([<<"SMEMBERS">>, ?REDIS_ALL_TASK(to_binary(Type))]),
    State = #state{tasks = Tasks, way = Way, node = to_binary(Type), live_ref = Ref},
    {ok, do_balance_task(State), 0}.

handle_call({add_task, Tasks}, _From, State) ->
    error_logger:info_msg("global_balance add_task ~p~n", [{Tasks, State#state.tasks}]),
    {State1, Result} = do_add_task(State, Tasks),
    {reply, Result, State1};
handle_call({del_task, Tasks}, _From, State) ->
    error_logger:info_msg("global_balance del_task ~p~n", [{Tasks, State#state.tasks}]),
    {State1, Result} = do_del_task(State, Tasks),
    {reply, Result, State1};
handle_call({syn_task, Tasks}, _From, State) ->
    error_logger:info_msg("global_balance sys_task ~p~n", [{Tasks, State#state.tasks}]),
    {State1, Result} = do_syn_task(State, Tasks),
    {reply, Result, State1};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_check_live(State),
    State2 = do_check_task(State1),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @doc call
%%------------------------------------------------------------------------------
do_add_task(#state{tasks = Tasks, node = Type} = State, AddTasks) ->
    case AddTasks -- Tasks of
        [] ->
            {State, ok};
        AddTasks1 ->
            case catch eredis_cluster:transaction([["SADD", ?REDIS_ALL_TASK(Type)] ++ AddTasks1]) of
                {ok, _} -> {State#state{tasks = AddTasks1 ++ Tasks, need_balance = true}, ok};
                Reason -> {State, {error, Reason}}
            end
    end.

do_del_task(#state{tasks = Tasks, node = Type} = State, DelTasks) ->
    case DelTasks -- (DelTasks -- Tasks) of
        [] ->
            {State, ok};
        DelTasks1 ->
            case catch eredis_cluster:transaction([["SREM", ?REDIS_ALL_TASK(Type)] ++ DelTasks1]) of
                {ok, _} -> {State#state{tasks = Tasks -- DelTasks1, need_balance = true}, ok};
                Reason -> {State, {error, Reason}}
            end
    end.

do_syn_task(#state{tasks = Tasks} = State, Tasks) -> {State, ok};
do_syn_task(#state{node = Type} = State, []) ->
    case catch eredis_cluster:q(["DEL", ?REDIS_ALL_TASK(Type)]) of
        {ok, _} -> {State#state{tasks = [], need_balance = true}, ok};
        Reason -> {State, {error, Reason}}
    end;
do_syn_task(#state{node = Type} = State, NewTasks) ->
    case catch eredis_cluster:transaction([["DEL", ?REDIS_ALL_TASK(Type)],
                                  ["SADD", ?REDIS_ALL_TASK(Type)] ++ NewTasks]) of
        {ok, _} -> {State#state{tasks = NewTasks, need_balance = true}, ok};
        Reason -> {State, {error, Reason}}
    end.

%%------------------------------------------------------------------------------
do_check_live(#state{live_ref = Ref} = State) ->
    case node_alive:get_ref() of
        {ok, NewRef} when NewRef =/= Ref ->
            error_logger:info_msg("global_balance find node_alive change ~p~n", [{NewRef, Ref}]),
            State#state{live_ref = NewRef, need_balance = true};
        _ ->
            State
    end.

do_check_task(#state{need_balance = false} = State) -> State;
do_check_task(State) ->
    case catch do_balance_task(State) of
        #state{} = State1 ->
            error_logger:info_msg("global_balance balance_task ok ~p~n", [{State1}]),
            State1#state{need_balance = false};
        Reason ->
            error_logger:error_msg("balance error ~p~n", [{Reason}]), State
    end.

%%------------------------------------------------------------------------------
%% @doc 平衡策略
%%------------------------------------------------------------------------------
do_balance_task(#state{way = Way} = State) ->
    case node_alive:get_nodes() of
        {ok, [_ | _] = Nodes} when Way =:= ?BALANCE_MODE_AVG ->
            do_avg_balance(State, Nodes);
        {ok, [_ | _] = Nodes} when Way =:= ?BALANCE_MODE_STD ->
            do_std_balance(State, Nodes);
        {ok, [_ | _] = Nodes} when Way =:= ?BALANCE_MODE_FXD ->
            do_fxd_balance(State, Nodes);
        _ ->
            State
    end.

%%------------------------------------------------------------------------------
do_avg_balance(#state{tasks = Tasks, node = Type} = State, Nodes) ->
    List = do_avg_split(Nodes, Tasks, length(Tasks) div length(Nodes), []),
    List1 = do_avg_from(Type, List, []),
    case eredis_cluster:transaction(List1) of
        {ok, _} -> State;
        {error, Reason} -> {error, Reason}
    end.

do_avg_split([Node | T], Tasks, Len, Acc) ->
    {NodeTask, Left} = lists:split(Len, Tasks),
    do_avg_split(T, Left, Len, [{Node, NodeTask} | Acc]);
do_avg_split([], Tasks, _Len, [{Node, NodeTask} | T]) ->
    [{Node, Tasks ++ NodeTask} | T].

do_avg_from(Type, [{Node, []} | T], Acc) ->
    do_avg_from(Type, T,
            [[<<"DEL">>, ?REDIS_NODE_TASK(Type, Node)] | Acc]);
do_avg_from(Type, [{Node, NodeTasks} | T], Acc) ->
    do_avg_from(Type, T,
            [[<<"DEL">>, ?REDIS_NODE_TASK(Type, Node)],
             [<<"SADD">>, ?REDIS_NODE_TASK(Type, Node)] ++ NodeTasks | Acc]);
do_avg_from(Type, [], Acc) ->
    [[<<"SET">>, ?REDIS_TASK_REF(Type), erlang:system_time()] | Acc].

%%------------------------------------------------------------------------------
do_std_balance(#state{tasks = Tasks, node = Type,
                      cache_tasks = OldTasks, cache_nodes = OldNodes} = State, Nodes) ->
    case lists:sort(OldNodes) =:= lists:sort(Nodes) of
        true ->
            {ok, _} = do_std_clean(OldTasks -- Tasks, Type, Nodes),
            {ok, _} = do_std_strafe(Tasks -- OldTasks, Type, Nodes),
            State#state{cache_tasks = Tasks, cache_nodes = Nodes};
        false ->
            List = do_avg_split(Nodes, Tasks, length(Tasks) div length(Nodes), []),
            List1 = do_avg_from(Type, List, []),
            case eredis_cluster:transaction(List1) of
                {ok, _} -> State#state{cache_tasks = Tasks, cache_nodes = Nodes};
                {error, Reason} -> {error, Reason}
            end
    end.

do_std_clean([], _Type, _Nodes) -> {ok, []};
do_std_clean(Dels, Type, Nodes) ->
    List = [[<<"SET">>, ?REDIS_TASK_REF(Type), erlang:system_time()]
            | [[<<"SREM">>, ?REDIS_NODE_TASK(Type, Node) | Dels] || Node <- Nodes]],
    eredis_cluster:transaction(List).

do_std_strafe([], _Type, _Nodes) -> {ok, []};
do_std_strafe(Adds, Type, Nodes) ->
    List = do_std_form(Adds, Type, Nodes),
    eredis_cluster:transaction(List).

do_std_form(Adds, Type, [Node]) ->
    [[<<"SET">>, ?REDIS_TASK_REF(Type), erlang:system_time()],
     [<<"SADD">>, ?REDIS_NODE_TASK(Type, Node) | Adds]];
do_std_form(Adds, Type, Nodes) ->
    List = [begin
                {ok, Cnt} = eredis_cluster:q([<<"SCARD">>, ?REDIS_NODE_TASK(Type, X)]),
                {binary_to_integer(Cnt), X}
            end || X <- Nodes],
    List1 = [{Min, _} | _] = lists:sort(List),
    do_fill_form(Adds, Type, [], Min, List1, []).

%% [2,3,4] -> [3,3,4] -> [4,4,4] -> [5,4,4]
do_fill_form([], Type, _Head, _Min, _Tail, Acc) ->
    [[<<"SET">>, ?REDIS_TASK_REF(Type), erlang:system_time()] | Acc];
do_fill_form(Adds, Type, [{Min, Node} | NT], Min, Tail, Acc) ->
    do_fill_form(Adds, Type, NT, Min, [{Min, Node} | Tail], Acc);
do_fill_form(Adds, Type, [{Cnt, _} | _] = Head, Min, Tail, Acc) when Cnt > Min ->
    do_fill_form(Adds, Type, Tail, Min + 1, Head, Acc);
do_fill_form(Adds, Type, [], Min, Tail, Acc) ->
    do_fill_form(Adds, Type, Tail, Min + 1, [], Acc);
do_fill_form([Add | AT], Type, [{Cnt, Node} | NT], Min, Tail, Acc) ->
    Acc1 = [[<<"SADD">>, ?REDIS_NODE_TASK(Type, Node), Add] | Acc],
    do_fill_form(AT, Type, [{Cnt + 1, Node} | NT], Min, Tail, Acc1).

%%------------------------------------------------------------------------------
do_fxd_balance(#state{tasks = Tasks, cache_tasks = OldTasks, node = Type} = State, Nodes) ->
    {ok, _} = do_std_clean(OldTasks -- Tasks, Type, Nodes),
    {ok, _} = do_std_strafe(Tasks -- OldTasks, Type, Nodes),
    State#state{cache_tasks = Tasks, cache_nodes = Nodes}.


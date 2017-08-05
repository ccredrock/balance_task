%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(global_balance).

-export([global_start/0, start_link/0]).

-export([get_tasks/0,
         get_redis_tasks/0]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-define(REDIS_HANDLE_REF(T),    iolist_to_binary([<<"$balance_task#handle_ref_">>, T])).
-define(REDIS_UPDATE_REF(T),    iolist_to_binary([<<"$balance_task#update_ref_">>, T])).
-define(REDIS_ALL_TASK(T),      iolist_to_binary([<<"$balance_task#all_task_">>, T])).
-define(REDIS_NODE_TASK(T, N),  iolist_to_binary([<<"$balance_task#node_task_">>, T, <<"_">>, N])).

-record(state, {tasks = [], task_ref = null, live_ref = null, node = null}).

%%------------------------------------------------------------------------------
global_start() ->
    ok = application:ensure_started(undead_global),
    ok = undead_global:reg(?MODULE, {?MODULE, start_link, []}).

get_tasks() ->
    State = sys:get_state(?MODULE), State#state.tasks.

get_redis_tasks() ->
    {ok, NodeType} = application:get_env(node_alive, node_type),
    {ok, Tasks} = eredis_pool:q([<<"SMEMBERS">>, ?REDIS_ALL_TASK(to_binary(NodeType))]), Tasks.

%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, Ref} = node_alive:get_ref(),
    {ok, NodeType} = application:get_env(node_alive, node_type),
    {ok, Tasks} = eredis_pool:q([<<"SMEMBERS">>, ?REDIS_ALL_TASK(to_binary(NodeType))]),
    State = #state{tasks = Tasks, node = to_binary(NodeType), live_ref = Ref},
    ok = do_balance_task(State),
    {ok, State, 0}.

handle_call({add_task, Tasks}, _From, State) ->
    {State1, Result} = do_add_task(State, Tasks),
    {reply, Result, State1};
handle_call({del_task, Tasks}, _From, State) ->
    {State1, Result} = do_del_task(State, Tasks),
    {reply, Result, State1};
handle_call({syn_task, Tasks}, _From, State) ->
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
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X.

%%------------------------------------------------------------------------------
do_check_live(#state{live_ref = Ref} = State) ->
    case node_alive:get_ref() of
        {ok, NewRef} when NewRef =/= Ref ->
            case do_balance_task(State) of
                ok -> State#state{live_ref = NewRef};
                _ -> State
            end;
        _ ->
            State
    end.

do_check_task(#state{node = NodeType, task_ref = Ref} = State) ->
    case eredis_pool:q([<<"GET">>, ?REDIS_UPDATE_REF(NodeType)]) of
        {ok, Ref} -> State;
        {error, _} -> State;
        {ok, NewRef} ->
            case do_balance_task(State) of
                ok -> State#state{task_ref = NewRef};
                _ -> State
            end
    end.

do_balance_task(#state{tasks = Tasks, node = Type}) ->
    case node_alive:get_nodes() of
        {ok, [_ | _] = Nodes} ->
            List = do_balance(Nodes, Tasks, length(Tasks) div length(Nodes), []),
            List1 = do_from(Type, List, []),
            case eredis_pool:transaction([["INCR", ?REDIS_HANDLE_REF(Type)] | List1]) of
                {ok, DD} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        _ ->
            ok
    end.

do_balance([Node | T], Tasks, Len, Acc) ->
    {NodeTask, Left} = lists:split(Len, Tasks),
    do_balance(T, Left, Len, [{Node, NodeTask} | Acc]);
do_balance([], Tasks, _Len, [{Node, NodeTask} | T]) ->
    [{Node, Tasks ++ NodeTask} | T].

do_from(Type, [{Node, []} | T], Acc) ->
    do_from(Type, T,
            [[<<"DEL">>, ?REDIS_NODE_TASK(Type, Node)] | Acc]);
do_from(Type, [{Node, NodeTasks} | T], Acc) ->
    do_from(Type, T,
            [[<<"DEL">>, ?REDIS_NODE_TASK(Type, Node)],
             [<<"SADD">>, ?REDIS_NODE_TASK(Type, Node)] ++ NodeTasks | Acc]);
do_from(_Type, [], Acc) -> Acc.

%%------------------------------------------------------------------------------
do_add_task(#state{tasks = Tasks, node = Type} = State, AddTasks) ->
    case AddTasks -- Tasks of
        [] ->
            {State, ok};
        AddTasks1 ->
            case eredis_pool:transaction([["SADD", ?REDIS_ALL_TASK(Type)] ++ AddTasks1,
                                          ["INCR", ?REDIS_UPDATE_REF(Type)]]) of
                {ok, _} -> {State#state{tasks = AddTasks1 ++ Tasks}, ok};
                {error, Reason} -> {State, {error, Reason}}
            end
    end.

do_del_task(#state{tasks = Tasks, node = Type} = State, DelTasks) ->
    case DelTasks -- (DelTasks -- Tasks) of
        [] ->
            {State, ok};
        DelTasks1 ->
            case eredis_pool:transaction([["SREM", ?REDIS_ALL_TASK(Type)] ++ DelTasks1,
                                          ["INCR", ?REDIS_UPDATE_REF(Type)]]) of
                {ok, _} -> {State#state{tasks = Tasks -- DelTasks1}, ok};
                {error, Reason} -> {State, {error, Reason}}
            end
    end.

do_syn_task(#state{tasks = Tasks} = State, Tasks) -> {State, ok};
do_syn_task(#state{node = Type} = State, []) ->
    case eredis_pool:transaction([["DEL", ?REDIS_ALL_TASK(Type)],
                                  ["INCR", ?REDIS_UPDATE_REF(Type)]]) of
        {ok, _} -> {State#state{tasks = []}, ok};
        {error, Reason} -> {State, {error, Reason}}
    end;
do_syn_task(#state{node = Type} = State, NewTasks) ->
    case eredis_pool:transaction([["DEL", ?REDIS_ALL_TASK(Type)],
                                  ["SADD", ?REDIS_ALL_TASK(Type)] ++ NewTasks,
                                  ["INCR", ?REDIS_UPDATE_REF(Type)]]) of
        {ok, _} -> {State#state{tasks = NewTasks}, ok};
        {error, Reason} -> {State, {error, Reason}}
    end.


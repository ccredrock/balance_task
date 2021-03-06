%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(balance_task).

-export([start/0]).


-export([add_task/1,
         del_task/1,
         syn_task/1,
         where_task/1]).

-export([get_tasks/0,
         get_alive_tasks/0,
         get_redis_tasks/0]).

%% callbacks
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-define(REDIS_TASK_REF(T),     iolist_to_binary([<<"${balance_task}#task_ref_">>, T])).
-define(REDIS_NODE_TASK(T, N), iolist_to_binary([<<"${balance_task}#node_task_">>, T, <<"_">>, N])).

-define(CATCH_RUN(X),
        case catch X of
            {'EXIT', Re} -> {error, Re};
            Rl -> Rl
        end).

-record(state, {node = {},  ref = 0, mod = null, tasks = []}).

%%------------------------------------------------------------------------------
start() ->
    application:ensure_all_started(?MODULE).

%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_task(binary() | [binary()]) -> ok | {error, any()}.
add_task(Task) when not is_list(Task) -> add_task([Task]);
add_task(Tasks) -> ?CATCH_RUN(gen_server:call(global:whereis_name(global_balance), {add_task, Tasks}, ?TIMEOUT)).

-spec del_task(binary() | [binary()]) -> ok | {error, any()}.
del_task(Task) when not is_list(Task) -> del_task([Task]);
del_task(Tasks) -> ?CATCH_RUN(gen_server:call(global:whereis_name(global_balance), {del_task, Tasks}, ?TIMEOUT)).

-spec syn_task([binary()]) -> ok | {error, any()}.
syn_task(Tasks) when is_list(Tasks) ->
    ?CATCH_RUN(gen_server:call(global:whereis_name(global_balance), {syn_task, Tasks}, ?TIMEOUT)).

-spec where_task(binary()) -> undefined | pid().
where_task(Task) ->
    case ets:lookup(?MODULE, Task) of
        [] -> undefined;
        [{_, PID}] -> PID
    end.

get_tasks() ->
    ?CATCH_RUN(element(#state.tasks, sys:get_state(?MODULE))).

get_alive_tasks() ->
    ets:tab2list(?MODULE).

get_redis_tasks() ->
    {ok, Tasks} = eredis_cluster:q([<<"SMEMBERS">>,
                                 ?REDIS_NODE_TASK(node_alive:node_type(),
                                                  node_alive:node_id())]), Tasks.

%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, Mod} = application:get_env(?MODULE, task_mod),
    ets:new(?MODULE, [named_table, public, {read_concurrency, true}]),
    {ok, #state{node = {node_alive:node_type(), node_alive:node_id()}, mod = Mod}, 0}.

handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_update(State),
    State2 = do_reborn(State1),
    erlang:send_after(?TIMEOUT, self(), timeout),
    {noreply, State2};
handle_info({'DOWN', _Ref, process, PID, Reason}, State) ->
    {noreply, do_dead(PID, Reason, State)};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
do_update(#state{ref = Ref, node = {NodeType, NodeID}, tasks = Tasks, mod = Mod} = State) ->
    case catch eredis_cluster:q([<<"GET">>, ?REDIS_TASK_REF(NodeType)]) of
        {ok, NewRef} when NewRef =/= Ref ->
            case catch eredis_cluster:q([<<"SMEMBERS">>, ?REDIS_NODE_TASK(NodeType, NodeID)]) of
                {ok, NewTasks} ->
                    Del = [do_stop(X, PID) || {X, PID} <- Tasks, not lists:member(X, NewTasks)],
                    Add = [do_start(Mod, X) || X <- NewTasks, lists:keyfind(X, 1, Tasks) =:= false],
                    State#state{tasks = (Tasks -- Del) ++ Add, ref = NewRef};
                _ ->
                    State
            end;
        _ ->
            State
    end.

do_stop(Task, PID) ->
    error_logger:info_msg("balance_task stop process ~p~n", [{Task, PID}]),
    ets:delete(?MODULE, Task),
    PID =/= null andalso exit(PID, shutdown),
    {Task, PID}.

do_start(Mod, Task) ->
    case catch Mod:start_link(Task) of
        {ok, PID} ->
            error_logger:info_msg("balance_task start process ~p~n", [{Task, Mod, PID}]),
            ets:insert(?MODULE, {Task, PID}),
            erlang:monitor(process, PID),
            {Task, PID};
        Reason ->
            error_logger:error_msg("balance_task start fail ~p~n", [{Task, Mod, Reason}]),
            {Task, null}
    end.

%%------------------------------------------------------------------------------
do_reborn(#state{mod = Mod, tasks = List} = State) ->
    State#state{tasks = [case PID of
                             null -> do_start(Mod, X);
                             _ -> {X, PID}
                         end || {X, PID} <- List]}.

%%------------------------------------------------------------------------------
do_dead(PID, Reason, #state{tasks = List} = State) ->
    case Reason =/= shutdown andalso Reason =/= kill of
        false -> State;
        true ->
            case lists:keyfind(PID, 2, List) of
                false -> State;
                {Task, PID} ->
                    error_logger:info_msg("balance_task process dead ~p~n", [{Task, PID}]),
                    State#state{tasks = lists:keystore(PID, 2, List, {Task, null})}
            end
    end.


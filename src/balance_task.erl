%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <meituan>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(balance_task).

-export([start/0, stop/0]).

-export([start_link/0]).

-export([add_task/1,
         del_task/1]).

-export([get_tasks/0,
         get_redis_tasks/0]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(TIMEOUT, 500).

-define(REDIS_HANDLE_REF(T), iolist_to_binary([<<"$balance_task#handle_ref_">>, T])).
-define(REDIS_NODE_TASK(T, N), iolist_to_binary([<<"$balance_task#node_task_">>, T, <<"_">>, N])).

-record(state, {node = {},  ref = 0, mod = null, tasks = []}).

%%------------------------------------------------------------------------------
start() ->
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_task(Task) ->
    catch gen_server:call(global:whereis_name(global_balance), {add_task, Task}).

del_task(Task) ->
    catch gen_server:call(global:whereis_name(global_balance), {del_task, Task}).

get_tasks() ->
    State = sys:get_state(?MODULE), State#state.tasks.

get_redis_tasks() ->
    {ok, NodeType} = application:get_env(node_alive, node_type),
    {ok, NodeID} = application:get_env(node_alive, node_id),
    {ok, Tasks} = eredis_pool:q([<<"SMEMBERS">>, ?REDIS_NODE_TASK(to_binary(NodeType), to_binary(NodeID))]), Tasks.

%%------------------------------------------------------------------------------
init([]) ->
    process_flag(trap_exit, true),
    {ok, Mod} = application:get_env(?MODULE, task_mod),
    {ok, NodeType} = application:get_env(node_alive, node_type),
    {ok, NodeID} = application:get_env(node_alive, node_id),
    {ok, #state{node = {to_binary(NodeType), to_binary(NodeID)}, mod = Mod}, 0}.

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
to_binary(X) when is_list(X)    -> list_to_binary(X);
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X.

%%------------------------------------------------------------------------------
do_update(#state{ref = Ref, node = {NodeType, NodeID}, tasks = Tasks, mod = Mod} = State) ->
    case eredis_pool:q([<<"GET">>, ?REDIS_HANDLE_REF(NodeType)]) of
        {ok, NewRef} when NewRef =/= Ref ->
            case eredis_pool:q([<<"SMEMBERS">>, ?REDIS_NODE_TASK(NodeType, NodeID)]) of
                {ok, NewTasks} ->
                    Del = [begin exit(PID, shutdown), {X, PID} end || {X, PID} <- Tasks, not lists:member(X, NewTasks)],
                    Add = [{X, do_start(Mod, X)} || X <- NewTasks, lists:keyfind(X, 1, Tasks) =:= false],
                    State#state{tasks = (Tasks -- Del) ++ Add, ref = NewRef};
                {error, _} ->
                    State
            end;
        _ ->
            State
    end.

do_start(Mod, Task) ->
    case Mod:start_link(Task) of
        {ok, PID} -> erlang:monitor(process, PID), PID;
        _ -> null
    end.

%%------------------------------------------------------------------------------
do_reborn(#state{mod = Mod, tasks = List} = State) ->
    State#state{tasks = [case PID of
                             null -> {X, do_start(Mod, X)};
                             _ -> {X, PID}
                         end || {X, PID} <- List]}.

%%------------------------------------------------------------------------------
do_dead(PID, Reason, #state{tasks = List} = State) ->
    case Reason =/= shutdown andalso Reason =/= kill of
        false -> State;
        true ->
            case lists:keyfind(PID, 2, List) of
                false -> State;
                {Task, PID} -> lists:keystore(PID, 2, List, {Task, null})
            end
    end.


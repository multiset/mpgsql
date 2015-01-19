-module(mpgsql_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([
    start_link/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(st, {
    conn,
    tx
}).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init(_) ->
    folsom_metrics:notify({mpgsql, worker_inits}, {inc, 1}),
    %% The only way to catch connection failures is to trap exits
    process_flag(trap_exit, true),
    {ok, #st{conn=undefined, tx=undefined}}.

handle_call(Msg, From, #st{conn=undefined}=State) ->
    case connect() of
        {ok, C} ->
            handle_call(Msg, From, State#st{conn=C});
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call({Ref, tx_begin}, {Pid, _}, #st{conn=C, tx=undefined}=State) ->
    %% Start a new transaction.
    case pgsql:squery(C, "BEGIN;") of
        {ok, [], []} ->
            MRef = erlang:monitor(process, Pid),
            {reply, ok, State#st{tx={Ref, MRef}}};
        Else ->
            {reply, Else, State}
    end;
handle_call({_Ref, tx_commit}, _From, #st{tx=undefined}=State) ->
    %% This is transaction-ending statement to a worker that wasn't in a
    %% transaction. Almost certainly a programming error.
    {reply, {error, no_transaction}, State};
handle_call({_Ref, tx_rollback}, _From, #st{tx=undefined}=State) ->
    %% This is transaction-ending statement to a worker that wasn't in a
    %% transaction. Almost certainly a programming error.
    {reply, {error, no_transaction}, State};
handle_call({_Ref, Msg}, _From, #st{tx=undefined}=State) ->
    handle_call_int(Msg, State);
handle_call({Ref, tx_begin}, _From, #st{conn=C, tx={Ref, MRef}}=State) ->
    %% This is a BEGIN from a process that's already in a transaction. Almost
    %% certainly a programming error, so abort. Shouldn't happen in production,
    %% but you never know.
    true = erlang:demonitor(MRef),
    {ok, [], []} = pgsql:squery(C, "ROLLBACK;"),
    {reply, {error, in_transaction}, State#st{tx=undefined}};
handle_call({Ref, tx_commit}, _From, #st{conn=C, tx={Ref, MRef}}=State) ->
    %% End the transaction.
    case pgsql:squery(C, "COMMIT;") of
        {ok, [], []} ->
            true = erlang:demonitor(MRef),
            {reply, ok, State#st{tx=undefined}};
        Else ->
            {reply, Else, State}
    end;
handle_call({Ref, tx_rollback}, _From, #st{conn=C, tx={Ref, MRef}}=State) ->
    %% End the transaction.
    case pgsql:squery(C, "ROLLBACK;") of
        {ok, [], []} ->
            true = erlang:demonitor(MRef),
            {reply, ok, State#st{tx=undefined}};
        Else ->
            {reply, Else, State}
    end;
handle_call({Ref, Msg}, _From, #st{tx={Ref, _}}=State) ->
    handle_call_int(Msg, State);
handle_call({RA, _}, _From, #st{conn=C, tx={RB, MRef}}=State) when RA =/= RB->
    %% This was a request for a transaction that was not our own.
    true = erlang:demonitor(MRef),
    {ok, [], []} = pgsql:squery(C, "ROLLBACK;"),
    {reply, {error, in_transaction}, State#st{tx=undefined}};
handle_call(Msg, _From, State) ->
    {stop, {unknown_call, Msg}, error, State}.

handle_call_int({equery, SQL, Fields}, #st{conn=C}=State) ->
    {reply, pgsql:equery(C, SQL, Fields), State};
handle_call_int({squery, SQL}, #st{conn=C}=State) ->
    {reply, pgsql:squery(C, SQL), State}.

handle_cast(Msg, State) ->
    {stop, {unknown_cast, Msg}, State}.

handle_info({'EXIT', From, Reason}, #st{conn=C}=State) when From =:= C ->
    case State#st.tx of
        undefined ->
            %% We weren't in a transaction, so we can gracefully recover later
            lager:warning(
                "Postgres connection (not in tx) died for reason ~p",
                [Reason]
            ),
            {noreply, State#st{conn=undefined}};
        _ ->
            %% This was our active connection, and we were in a transaction! Die
            %% to ensure that our owner doesn't erroneously assume that the
            %% transaction completed.
            lager:warning(
                "Postgres connection (in tx) died for reason ~p",
                [Reason]
            ),
            {stop, connection_died, State}
    end;
handle_info({'EXIT', _From, _Reason}, State) ->
    %% This message was probably generated by a failure in connect/0 and can
    %% safely be ignored.
    {noreply, State};
handle_info({'DOWN', Ref, process, _, _}, #st{tx={_, Ref}}=State) ->
    {noreply, State#st{tx=undefined}};
handle_info(Msg, State) ->
    {stop, {unknown_info, Msg}, State}.

terminate(_Reason, #st{conn=Conn}) ->
    pgsql:close(Conn),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec connect() -> {ok, Connection} | {error, Reason} when
    Connection :: pid(),
    Reason :: any().

connect() ->
    {ok, Hostname} = application:get_env(mpgsql, hostname),
    {ok, Port} = application:get_env(mpgsql, port),
    {ok, Database} = application:get_env(mpgsql, database),
    {ok, Username} = application:get_env(mpgsql, username),
    {ok, Password} = application:get_env(mpgsql, password),
    Opts = [{port, Port}, {database, Database}],
    try pgsql:connect(Hostname, Username, Password, Opts)
    catch exit:{Reason, _} ->
        lager:warning(
            "mpgsql worker failed to connect for reason ~p",
            [Reason]
        ),
        {error, Reason}
    end.
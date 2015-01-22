-module(mpgsql).

-export([
    tx_begin/0,
    tx_begin/1,
    tx_commit/0,
    tx_commit/1,
    tx_rollback/0,
    tx_rollback/1,
    equery/2,
    equery/3,
    squery/1,
    squery/2
]).

-define(WORKER, mpgsql_worker).
-define(TXREF, mpgsql_tx_ref).

-include("mpgsql.hrl").

%%
%% Transaction management functions go directly to query_int because it doesn't
%% make sense for them to auto-vivify a worker if there isn't one already.
%%

tx_begin() ->
    {ok, Timeout} = application:get_env(mpgsql, query_timeout),
    tx_begin(Timeout).

-spec tx_begin(Timeout) -> ok | {error, Error} when
    Timeout :: non_neg_integer(),
    Error :: in_transaction | noproc | timeout.

tx_begin(Timeout) ->
    case {erlang:get(?WORKER), erlang:get(?TXREF)} of
        {undefined, undefined} ->
            case checkout(Timeout) of
                {error, _}=Error ->
                    Error;
                {ok, CheckoutTime} ->
                    Ref = erlang:make_ref(),
                    erlang:put(?TXREF, Ref),
                    RemainingTime = trunc(Timeout - CheckoutTime),
                    case query_int(tx_begin, RemainingTime) of
                        ok ->
                            ok;
                        Else ->
                            %% The transaction failed to start, so kill the ref
                            erlang:erase(?TXREF),
                            Else
                    end
            end;
        _Else ->
            {error, in_transaction}
    end.

tx_commit() ->
    {ok, Timeout} = application:get_env(mpgsql, query_timeout),
    tx_commit(Timeout).

-spec tx_commit(Timeout) -> ok | {error, Error} when
    Timeout :: non_neg_integer(),
    Error :: no_tx | noproc | timeout.

tx_commit(Timeout) ->
    tx_end(tx_commit, Timeout).

tx_rollback() ->
    {ok, Timeout} = application:get_env(mpgsql, query_timeout),
    tx_rollback(Timeout).

-spec tx_rollback(Timeout) -> ok | {error, Error} when
    Timeout :: non_neg_integer(),
    Error :: no_transaction | noproc | timeout.

tx_rollback(Timeout) ->
    tx_end(tx_rollback, Timeout).

tx_end(Cmd, Timeout) ->
    case {erlang:get(?WORKER), erlang:get(?TXREF)} of
        {undefined, _} ->
            {error, no_transaction};
        {_, undefined} ->
            {error, no_transaction};
        _Else ->
            try query_int(Cmd, Timeout) of
                ok ->
                    ok = checkin(),
                    ok;
                Else ->
                    Else
            after
                %% Ensure that the ref is closed even if the sqeruy fails
                erlang:erase(?TXREF)
            end
    end.

%%
%% Basic entry points. These will automatically check out/check in a worker if
%% the requesting process hasn't already done so.
%%

equery(SQL, Fields) ->
    query({equery, SQL, Fields}).

equery(SQL, Fields, Timeout) ->
    query({equery, SQL, Fields}, Timeout).

squery(SQL) ->
    query({squery, SQL}).

squery(SQL, Timeout) ->
    query({squery, SQL}, Timeout).

query(Msg) ->
    {ok, Timeout} = application:get_env(mpgsql, query_timeout),
    query(Msg, Timeout).

query(Msg, Timeout) ->
    case erlang:get(?WORKER) of
        undefined ->
            case checkout(Timeout) of
                {error, _}=Error ->
                    Error;
                {ok, CheckoutTime} ->
                    R = query_int(Msg, trunc(Timeout - CheckoutTime)),
                    ok = checkin(),
                    R
            end;
        _ ->
            query_int(Msg, Timeout)
    end.

query_int(Msg, Timeout) ->
    Worker = erlang:get(?WORKER),
    Ref = erlang:get(?TXREF),
    try gen_server:call(Worker, {Ref, Msg}, Timeout) of
        {error, {error, error, <<"23505">>, _, _}} ->
            {error, unique_violation};
        {error, {error, error, <<"23503">>, _, _}} ->
            {error, foreign_key_violation};
        Other ->
            Other
    catch
        exit:{timeout, _} ->
            %% We want to consider the worker dead, since it's potentially
            %% in an infinite wait for a socket response that might not ever
            %% arrive. Therefore, we simply kill it.
            ?INCREMENT_COUNTER([mpgsql, queries, timeouts]),
            exit(Worker, kill),
            checkin(),
            {error, timeout};
        exit:{noproc, _} ->
            %% Our client died (or killed itself) already, or we never had one.
            ?INCREMENT_COUNTER([mpgsql, workers, dead]),
            checkin(),
            {error, noproc}
    end.


%%
%% N.B.: This logic assumes that poolboy is *always* going to be responsive - we
%% don't handle gen_server:call/3 timeouts here. The use of non-blocking poolboy
%% interactions *should* more or less guarantee that this is okay.
%%

checkout(Timeout) ->
    {ok, Backoff} = application:get_env(mpgsql, checkout_backoff),
    checkout(Timeout, Backoff).

-spec checkout(Timeout, Backoff) -> {ok, TimeDelta} | {error, Error} when
    Timeout :: non_neg_integer(),
    Backoff :: non_neg_integer(),
    TimeDelta :: non_neg_integer(),
    Error :: timeout.

checkout(Timeout, Backoff) ->
    Now = os:timestamp(),
    random:seed(Now),
    checkout(Now, Timeout, Backoff).

checkout(StartTime, Timeout, Backoff0) when Timeout > 0 ->
    case poolboy:checkout(mpgsql, false, Timeout) of
        full ->
            Backoff1 = trunc(Backoff0 * (1 + random:uniform())),
            timer:sleep(min(Backoff1, Timeout)),
            checkout(StartTime, Timeout - Backoff1, Backoff1);
        Worker ->
            Delta = timer:now_diff(os:timestamp(), StartTime) div 1000,
            ?UPDATE_HISTOGRAM([mpgsql, workers, checkout_time], Delta),
            erlang:put(?WORKER, Worker),
            {ok, Delta}
    end;
checkout(_, _, _) ->
    {error, timeout}.

-spec checkin() -> {error, no_worker} | ok.

checkin() ->
    case erlang:get(?WORKER) of
        undefined ->
            {error, no_worker};
        W ->
            ok = poolboy:checkin(mpgsql, W),
            erlang:erase(?WORKER),
            ok
    end.

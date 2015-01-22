-module(mpgsql_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    folsom_metrics:new_counter([mpgsql, workers, dead]),
    folsom_metrics:new_counter([mpgsql, workers, initialized]),
    folsom_metrics:new_histogram(
        [mpgsql, workers, checkout_time],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_counter([mpgsql, connections, initialized]),
    folsom_metrics:new_counter([mpgsql, connections, initialization_failures]),
    folsom_metrics:new_counter([mpgsql, connections, terminated]),
    folsom_metrics:new_counter([mpgsql, transactions, begins, success]),
    folsom_metrics:new_counter([mpgsql, transactions, begins, failure]),
    folsom_metrics:new_counter([mpgsql, transactions, commits, success]),
    folsom_metrics:new_counter([mpgsql, transactions, commits, failure]),
    folsom_metrics:new_counter([mpgsql, transactions, rollbacks, success]),
    folsom_metrics:new_counter([mpgsql, rejects]),
    folsom_metrics:new_counter([mpgsql, queries, squery, count]),
    folsom_metrics:new_counter([mpgsql, queries, equery, count]),
    folsom_metrics:new_counter([mpgsql, queries, tx, count]),
    folsom_metrics:new_counter([mpgsql, queries, notx, count]),
    folsom_metrics:new_counter([mpgsql, queries, count]),
    folsom_metrics:new_counter([mpgsql, queries, timeouts]),
    folsom_metrics:new_histogram(
        [mpgsql, queries, squery, latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        [mpgsql, queries, equery, latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        [mpgsql, queries, tx, latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        [mpgsql, queries, notx, latency],
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        [mpgsql, queries, latency],
        slide_uniform,
        {10, 1024}
    ),
    {ok, Size} = application:get_env(mpgsql, pool_size),
    PoolArgs = [
        {name, {local, mpgsql}},
        {worker_module, mpgsql_worker},
        {max_overflow, 0},
        {size, Size}
    ],
    Spec = poolboy:child_spec(mpgsql, PoolArgs, []),
    {ok, {{one_for_one, 100, 1}, [Spec]}}.

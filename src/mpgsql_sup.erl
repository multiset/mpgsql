-module(mpgsql_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    folsom_metrics:new_histogram(
        {mpgsql, checkout_time},
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_histogram(
        {mpgsql, query_time},
        slide_uniform,
        {10, 1024}
    ),
    folsom_metrics:new_counter({mpgsql, worker_inits}),
    {ok, Size} = application:get_env(mpgsql, pool_size),
    PoolArgs = [
        {name, {local, mpgsql}},
        {worker_module, mpgsql_worker},
        {max_overflow, 0},
        {size, Size}
    ],
    Spec = poolboy:child_spec(mpgsql, PoolArgs, []),
    {ok, {{one_for_one, 100, 1}, [Spec]}}.

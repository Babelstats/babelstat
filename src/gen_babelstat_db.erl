-module(gen_babelstat_db).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{query_database, 1}];
behaviour_info(_) ->
    undefined.

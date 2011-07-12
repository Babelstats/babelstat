%%%-------------------------------------------------------------------
%%% @author nisbus <>
%%% @copyright (C) 2011, nisbus
%%% @doc
%%%
%%% @end
%%% Created :  9 Jul 2011 by nisbus <>
%%%-------------------------------------------------------------------
-module(babel_calc).
-include("../include/babelstat.hrl").
%% API
-export([calculate/1,eval/1]).
%%%===================================================================
%%% API
%%%===================================================================
-spec eval(Algebra :: string()) -> float().
eval(Algebra) ->
    {ok,Ts,_} = calc_lexer:string(Algebra),
    {ok, Result} = calc_parser:parse(Ts),
    Result.

-spec calculate(Series :: [{calendar:t_datetime1970(),string()}]) -> [{calendar:t_datetime1970(),float()}].
calculate(Series)->
    lists:map(fun({Date,Algebra}) ->
			 Result = babel_calc:eval(Algebra),
			 {Date,Result}
	      end, Series).

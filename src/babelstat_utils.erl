%%%-------------------------------------------------------------------
%%% @author nisbus <>
%%% @copyright (C) 2011, nisbus
%%% @doc
%%%
%%% @end
%%% Created :  9 Jul 2011 by nisbus <>
%%%-------------------------------------------------------------------
-module(babelstat_utils).
-include("../include/babelstat.hrl").
%% API
-export([transpose/1, date_adjust/6,convert_metric/3, convert_scale/3,convert_docs_to_series/4,create_constants_series/5]).
-export([replace_token_with_value/3,parse_calculation/1,replace_tokens_with_values/2,create_legend/2,function_operators/0,is_string_number/1]).

function_operators() ->
    ["pi","sqrt","sin","cos","tan","asin","acos","atan","sinh","cosh","tanh","asinh","acosh", "atanh","exp","log","erf","erfc"].
%%%===================================================================
%%% API
%%%===================================================================
%%@doc Transposes a list of lists.
-spec transpose([list()]) -> [list()].
transpose([]) -> [];
transpose([Single,[]]) -> Single;
transpose([H|_]=L) -> [lists:map(F, L) || F <- [fun(A) -> lists:nth(N, A) end || N <- lists:seq(1, length(H))]].

%%@doc Aggregates documents to a common timeframe (Needs optimizing)
-spec date_adjust(Values::[float()],Dates::[babel_date()], Frequency::frequency(), Docs::[db_result()],
		  StartDate::babel_date(), EndDate::babel_date()) -> {Values::[float()],Dates::[babel_date()], Docs::[db_result()]}.
date_adjust(Values, Dates, Frequency, Docs, StartDate, EndDate) ->	
    case lists:any(fun(D) -> D#babelstat.frequency =/= Frequency end,Docs) of
    	true ->	    
    	    %%Dates need to be filtered (aggregated to the frequency)
    	    ValidDates = date_range:create_range(StartDate, EndDate, Frequency),
    	    Zipped = lists:zip3(Values,Dates,Docs),
	    {_,MatchedList} = lists:foldl(fun(N,Acc) ->
					   {Counter, List} = Acc,
					   {Match,_Dont} = lists:partition(fun(P) ->
										   {_,D,_} = P,
										   Range = lists:sublist(ValidDates,N,N+1),
										   is_date_in_range(Range, D)
								      end,lists:sublist(Zipped,Counter+1,length(Zipped))),
						  case length(Match) of 
						      0-> Acc;
						      _ ->
							  {Counter + length(Match),[aggregate_docs(Match,Frequency)|List]}
						  end
				   end,{0,[]},lists:seq(1,length(ValidDates))),
	    MatchedList;
	_ ->
	    {Values,Dates,Docs}
    end.

-spec convert_docs_to_series(#babelstat_query{}, #babelstat_filter{},
			     {Values::[float()], Dates::[calendar:t_datetime1970()]}, Docs::[#babelstat{}]) ->
				   #babelstat_series{}.
convert_docs_to_series(#babelstat_query{ category = Category,
					 sub_category = Sub_Category,
					 subject = Subject,
					 series_category = Series_Category,
					 title = Title } = Params, 
		       #babelstat_filter{ metric = Metric,
					  scale = Scale,
					  frequency = Frequency} = Filter, {Values, _Dates}, Docs) ->
    {ConvertedValues,_} = lists:foldl(fun(Doc, Acc) ->
					      {NewValues, Counter} = Acc,
					      DocScale = Doc#babelstat.scale,
					      DocMetric = Doc#babelstat.metric,
					      Value = lists:nth(Counter+1,Values),      
					      NewValue =  convert_scale(DocScale, Scale, Value),
					      Converted = convert_metric(DocMetric, Metric,NewValue),
					      {NewValues++[{Doc#babelstat.date,Converted}],Counter+1} 
		       end,{[],0},Docs),

    #babelstat_series{series = ConvertedValues, metric = Metric, scale = Scale, 
		      frequency = Frequency, category = Category, sub_category = Sub_Category, 
		      subject = Subject, series_category = Series_Category, title = Title, 
		      legend = create_legend(Params,Filter)}.

-spec create_constants_series(#babelstat_query{},
			      #babelstat_filter{}, Value :: float(), DocScale :: integer(),
			      DocMetric :: binary() | atom()) -> #babelstat_series{}.
create_constants_series(#babelstat_query{ category = Category,
					  sub_category = SubCategory,
					  subject = Subject,
					  series_category = SeriesCategory,
					  title = Title } = Params,
			#babelstat_filter{ metric = Metric,
					   scale = Scale,
					   frequency = Frequency,
					   from_date = From,
					   to_date = To} = Filter, Value, DocScale, DocMetric) ->
    DateList = dates:create_range(From, To, Frequency),
    ConstantSeries = #babelstat_series{metric = Metric, scale = Scale, frequency = Frequency,
				      category = Category, sub_category = SubCategory, subject = Subject,
				      series_category = SeriesCategory, title = Title, 
				      legend = create_legend(Params,Filter)},
    ValuesAndDates = lists:map(fun(Date) ->
			       NewValue = convert_scale(DocScale, Scale, Value),
			       {Date,convert_metric(DocMetric, Metric, NewValue)}
		       end,DateList),
    ConstantSeries#babelstat_series{series = ValuesAndDates}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
aggregate_docs(DocList,NewFreq) ->
    lists:foldl(fun(O,Acc) ->
			{NoV, _,_} = Acc,
			{V,D,Doc} = O,
			{V+NoV, D,Doc#babelstat{frequency = NewFreq}}
		end,{0.0,undefined,undefined},DocList).


-spec create_legend(#babelstat_query{}, #babelstat_filter{}) ->
			   binary().
create_legend(#babelstat_query{ category = Category,
				sub_category = SubCategory,
				subject = Subject,
				series_category = SeriesCategory,
				title = Title },
	      #babelstat_filter{ metric = Metric }) ->
    Sep = <<" - ">>,
    Metric0 = <<"(",Metric/binary,")">>,
    <<Category/binary, Sep/binary, SubCategory/binary, Sep/binary, Subject/binary, Sep/binary, SeriesCategory/binary,
      Sep/binary, Title/binary,Sep/binary, Metric0/binary>>.

%%%===================================================================
%%% Date helper functions
%%%===================================================================
is_date_in_range(Range, Date)->
    case length(Range) of 
	0 ->
	    false;
	1 ->
	    Date =< hd(Range);	    
	2 ->
	    From = hd(Range),
	    To = hd(lists:reverse(Range)),
	    case {{Date >= From},{Date =< To}} of
		{{true},{true}} ->
		    true;
		{_,_} ->
		    false
	    end;
	_ ->
	    false
    end.
	    
%%%===================================================================
%%% Metric and scale helper functions
%%%===================================================================
-spec convert_metric(binary(), binary(), float()) -> float().
convert_metric(OriginalMetric, NewMetric, Value) ->
    measurements:convert(binary_to_list(OriginalMetric), binary_to_list(NewMetric), Value).

-spec convert_scale(integer(), integer(), float()) -> float().
convert_scale(OriginalScale, NewScale, Value) ->
    case OriginalScale =:= NewScale of
	true ->
	    Value;
	false ->
	    case OriginalScale < NewScale of
		true ->
		    Value*(OriginalScale/NewScale);
		false ->
		    Value*(NewScale/OriginalScale)
	    end
    end.

%%%===================================================================
%%% Metric and scale helper functions
%%%===================================================================
-spec parse_calculation(binary()) -> {[#babelstat_query{}], string()}.		       
parse_calculation(Calculation1) ->
    Calculation = binary_to_list(Calculation1),
    TokenList = string:tokens(Calculation,"()+-/*^"),
    Tokens = lists:foldl(fun(X,Acc) ->
				 case lists:member(X,function_operators()) of
				     true ->
					 Acc;
				     false ->
					 case is_string_number(X) of 
					     true ->
						 Acc;
					     false ->
						 Acc++[X]
					 end
				 end 
			 end,[],TokenList),
			    
    error_logger:info_msg("TokenList ~p~n",[TokenList]),
    error_logger:info_msg("Tokens ~p~n",[Tokens]),
    PrettyAlgebra = simplify_algebra(Tokens,Calculation),
    error_logger:info_msg("Pretty ~p~n",[PrettyAlgebra]),
    Queries = lists:map(fun(Token) ->
				Items = string:tokens(Token,"{,}"),
				{C,SuC,Subj,Sc,T} = list_to_tuple(Items),
				#babelstat_query{ category = list_to_binary(C),
						  sub_category = list_to_binary(SuC),
						  subject = list_to_binary(Subj),
						  series_category = list_to_binary(Sc),
						  title = list_to_binary(T)
						}
			end,Tokens),
    {Queries, PrettyAlgebra}.
    
is_string_number(X) ->
    case catch list_to_integer(X) of
	{'EXIT',_} ->
	    case catch list_to_float(X) of
		{'EXIT',_} ->
		    false;
		_ ->
		    true
	    end;
	_ ->
	    true
    end.

-spec replace(string(),string(), string()) -> string().
replace(Original, ToReplace, ReplaceWith) ->
    Index = string:str(Original,ToReplace),
    Len = length(ToReplace),
    LeftSide = string:substr(Original,1,Index-1),
    RightSide = string:substr(Original,Index+Len),
    lists:append([LeftSide,ReplaceWith,RightSide]).


-spec replace_tokens_with_values(Albegra::string(), List::[float()]) -> [string()].					
replace_tokens_with_values(Algebra,List) ->
    Tokens = string:tokens(Algebra,"()+-/*^"),
    %Get the values from the list
    Lists1 = lists:map(fun(#babelstat_series{series = Serie}) ->
			       lists:map(fun({Date,Value}) ->
						{Date, Value}
					 end,Serie)			       
		       end, List),
    Transposed = transpose(Lists1),
    R = lists:map(fun(X) ->
			  {Date,_} = hd(X),
			  Folded = lists:foldl(fun({_D,Y},Acc) ->					      
					      {A,Counter} = Acc,
					      Token = lists:nth(Counter,Tokens),
					      Replaced = replace_token_with_value(A, Token, [Y]),
					      {Replaced,Counter+1}
				      end,{Algebra, 1},X),		  
			  {Date,Folded}
		  end,Transposed),
    [{D,Result} || {D,{Result,_}} <- R].
    
replace_token_with_value(Original, ToReplace, ReplaceWith) ->
    R = case ReplaceWith of
	X when is_float(X) ->
		X;
	[Y] ->
	        Y*1.0
	end,
    Index = string:str(Original,ToReplace),
    Len = length(ToReplace),
    LeftSide = string:substr(Original,1,Index-1),
    RightSide = string:substr(Original,Index+Len),
    [Float] = io_lib:format("~.6f",[R]),
    LeftSide++Float++RightSide.

-spec simplify_algebra(string(),string()) -> string().			  
simplify_algebra(Tokens,Calculation) ->
    TokenCount = length(Tokens),    
    case TokenCount > 26 of
	true ->
	    erlang:error("this version of BabelStat only supports calculations of up to 26 variables (UPPERCASE ASCII)");
	false ->
	    lists:foldl(fun(N,Acc) ->
				Char = integer_to_list(64+N),
				Token = lists:nth(N,Tokens),
				replace(Acc,Token,Char)
			end,Calculation,lists:seq(1,TokenCount))
    end.


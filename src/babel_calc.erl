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
-export([eval/1,query_db/2]).
-export([test_calculation_parser/0,replace_token_with_value/3,test_query/0,test_query/3, date_adjust/6,to_babelstat_records/1,test_dates/0]).

%%%===================================================================
%%% API
%%%===================================================================
eval(Algebra) ->
    {ok,Ts,_} = calc_lexer:string(Algebra),
    calc_parser:parse(Ts).

transpose([]) -> [];
transpose([Single,[]]) -> Single;
transpose([H|_]=L) -> [lists:map(F, L) || F <- [fun(A) -> lists:nth(N, A) end || N <- lists:seq(1, length(H))]].

get_documents(_Params,_Filter) ->
    %%Call db for the view    
    to_babelstat_records([]).
  
%%@doc queries the database recursively and returs a babelstat_series
-spec query_db(Params :: list(), Filter :: tuple()) -> #babelstat_series{}.		      
query_db(Params, {_,_,FilterFrequency, From, To}= Filter) ->
	View = get_documents(Params,Filter),
	case View of
	    [Doc] ->
		%%Single document returned, either it is a constant or a calculation
		case Doc#babelstat.constant of
		    true ->
			create_constants_series(Params, Filter,Doc#babelstat.value,Doc#babelstat.scale,Doc#babelstat.metric);
		    false ->
			%%it's a calculation
			Calc = Doc#babelstat.calculation,
			{Queries, Algebra} = parse_calculation(Calc),
			Series = lists:map(fun(Serie) ->
					  query_db(Serie,Filter)
				  end,Queries),
			calculate(replace_tokens_with_values(Series,Algebra))			
		end;
	    [_H|_T] = Docs ->
		{Dates, Values} = lists:foldl(fun(Doc,Acc) ->
						     {Dates, Values} = Acc,
						     {Dates++[Doc#babelstat.date],Values++[Doc#babelstat.value]}
					     end,{[],[]},Docs),
		{FilteredValues, FilteredDates,FilteredDocs} = date_adjust(Values,Dates,Docs, FilterFrequency,From, To),
		convert_docs_to_series(Params, Filter, {FilteredValues,FilteredDates}, FilteredDocs);
	    _ ->
		{error, no_documents_found}
	end.
			
date_adjust(Values, Dates, Frequency, Docs, StartDate, EndDate) ->	
    {Values,Dates,Docs}.
    %% case lists:any(fun(D) -> D#babelstat.frequency =/= Frequency end,Docs) of
    %% 	true ->	    
    %% 	    ParsedStart = parse_date(list_to_binary(StartDate)),
    %% 	    ParsedEnd = parse_date(list_to_binary(EndDate)),
    %% 	    io:format("Frequency mismatch, aggregating~n"),
    %% 	    %%Dates need to be filtered (aggregated to the frequency)
    %% 	    ValidDates = date_range:create_range(ParsedStart,ParsedEnd, Frequency),
    %% 	    Zipped = lists:zip3(Values,Dates,Docs),
    %% 	    lists:foldl(fun(N,_Acc) ->
    %% 				{_Match,_Dont} = lists:partition(fun(P) ->
    %% 								       {_,D,_} = P,
    %% 								       Range = lists:sublist(ValidDates,N,N+1),
    %% 								       Dt = parse_date(list_to_binary(D)),
    %% 								       is_date_in_range(Range, Dt)
    %% 							       end,Zipped) 
    %% 				{0.0,undefined,undefined},lists:seq(1,length(ValidDates)) end);
			    
			   


%% 	    R = [lists:map(F,Zipped) || F= [fun(Z) -> 
%% 						    io:format("Sub ~p~n",[Z]),
%% 						    lists:foldl(fun(Y,Sum) ->
%% 	    								io:format("Sum ~p~n",[Sum]),
%% %	    								{V,_,_} = Sum,
%% 	    								io:format("Y ~p~n",[Y])
%% 	    								%% {V2,D2,Doc2} = Y,
%% 	    								%% {V+V2,D2,Doc2}
%% 	    							end,{0.0,[],[]},lists:partition(fun(P) ->
%% 													io:format("P = ~p~n",[P]),
%% 	    												{_,D,_} = P,
%% 	    												Range = lists:sublist(ValidDates,N,N+1),
%% 	    												is_date_in_range(Range,D)
%% 	    											end,Zipped))
%% 	    				end || N <- lists:seq(1,length(ValidDates))]],
%%	    io:format("Beautiful ~p~n",[R]);
    %% 	false ->
    %% 	    {Values,Dates,Docs}
    %% end.

is_date_in_range(Range, Date)->
    io:format("Range and date comparison ~p ~p~n",[Range,Date]),
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
		    io:format("Is in range~n"),
		    true;
		{_,_} ->
		    io:format("Not in range~n"),
		    false
	    end;
	_ ->
	    false
    end.
%% is_date_in_range(Range,D) ->    
%%     Date = parse_date(list_to_binary(D)),
%%     case length(Range) of 
%% 	0 ->
%% 	    false;
%% 	1 ->
%% 	    Date =< parse_date(list_to_binary(hd(Range)));	    
%% 	2 ->
%% 	    From = parse_date(list_to_binary(hd(Range))),
%% 	    To = parse_date(list_to_binary(hd(lists:reverse(Range)))),
%% 	    case {{Date >= From},{Date =< To}} of
%% 		{true,true} ->
%% 		    true;
%% 		{_,_} ->
%% 		    false
%% 	    end;
%% 	_ ->
%% 	    false
%%     end.
    
	    
parse_date(<<Y:4/binary,"-",M:1/binary,"-",D:1/binary>>) ->
    {_,{H,Min,Sec}} = erlang:localtime(),
    {
      {list_to_integer(binary_to_list(Y)),
       list_to_integer(binary_to_list(M)),
       list_to_integer(binary_to_list(D))},
       {H,Min,Sec}
      };

parse_date(<<Y:4/binary,"-",M:1/binary,"-",D:2/binary>>) ->
    {_,{H,Min,Sec}} = erlang:localtime(),
      {{list_to_integer(binary_to_list(Y)),
      list_to_integer(binary_to_list(M)),
       list_to_integer(binary_to_list(D))},
       {H,Min,Sec}};

parse_date(<<Y:4/binary,"-",M:2/binary,"-",D:2/binary>>) ->
    {_,{H,Min,Sec}} = erlang:localtime(),
      {{list_to_integer(binary_to_list(Y)),
       list_to_integer(binary_to_list(M)),
       list_to_integer(binary_to_list(D))},
       {H,Min,Sec}};
parse_date(<<Y:4/binary,"-",M:2/binary,"-",D:2/binary," ",H:2/binary,":",Min:2/binary,":",Sec:2/binary>>) ->
      {{list_to_integer(binary_to_list(Y)),
       list_to_integer(binary_to_list(M)),
	list_to_integer(binary_to_list(D))},
       {list_to_integer(binary_to_list(H)),
	list_to_integer(binary_to_list(Min)),
	list_to_integer(binary_to_list(Sec))}}.
    

convert_docs_to_series([Category, Sub_Category, Subject, Series_Category, Title] = Params, {Metric, Scale, Frequency, _, _} = Filter, {Values, Dates}, Docs) ->
    {ConvertedValues,_} = lists:foldl(fun(Doc, Acc) ->
					      {NewValues, Counter} = Acc,
					      DocScale = Doc#babelstat.scale,
					      DocMetric = Doc#babelstat.metric,
					      Value = lists:nth(Counter+1,Values),      
					      NewValue =  convert_scale(DocScale, Scale, Value),
					      Converted = convert_metric(DocMetric, Metric,NewValue),
					      {NewValues++[Converted],Counter+1} 
		       end,{[],0},Docs),

    #babelstat_series{dates = Dates, values = ConvertedValues, metric = Metric, scale = Scale, 
		      frequency = Frequency, category = Category, sub_category = Sub_Category, 
		      subject = Subject, series_category = Series_Category, title = Title, 
		      legend = create_legend(Params,Filter)}.
    

create_constants_series([Category, Sub_Category, Subject, Series_Category, Title] = Params, {Metric, Scale, Frequency, From, To} = Filter, Value, DocScale,DocMetric) ->
    DateList = dates:create_range(From,To, list_to_atom(Frequency)),
    ConstantSeries = #babelstat_series{dates = DateList, metric = Metric, scale = Scale, frequency = Frequency,
				      category = Category, sub_category = Sub_Category, subject = Subject,
				      series_category = Series_Category, title = Title, 
				      legend = create_legend(Params,Filter)},
    Values = lists:map(fun(_Date) ->
			       NewValue = convert_scale(DocScale, Scale, Value),
			       convert_metric(DocMetric, Metric,NewValue)      
		       end,DateList),
    ConstantSeries#babelstat_series{values = Values}.

-spec create_legend(list(),tuple()) -> string().
create_legend([Category, Sub_Category, Subject, Series_Category, Title], {Metric, _, _, _, _}) ->
    Sep = " - ",
    lists:append([Category, Sep, Sub_Category, Sep, Subject, Sep, Series_Category, Sep, Title, " (",Metric,")"]).


-spec convert_metric(float(),float(), float()) -> float().
convert_metric(OriginalMetric,NewMetric,Value) ->
    measurements:convert(OriginalMetric, NewMetric, Value).

-spec convert_scale(float(),float(), float()) -> float().
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

-spec parse_calculation(string()) -> {list(),string()}.		       
parse_calculation(Calculation) ->
    Tokens = string:tokens(Calculation,"()+-/*^"),
    PrettyAlgebra = simplify_algebra(Tokens,Calculation),
    Queries = lists:map(fun(Token) ->
				Items = string:tokens(Token,"{,}"),
				{C,SuC,Subj,Sc,T} = list_to_tuple(Items),
				[C,SuC,Subj,Sc,T]
			end,Tokens),
    {Queries, PrettyAlgebra}.

test_calculation_parser() ->
    Test = "{Cat,SubC,Subj,Sc,T}+{Cat2,SubC2,Subj2,Sc2,T2}",
    {_,A} = parse_calculation(Test),
    replace_tokens_with_values(A,[[50.0],[50.0]]).
    
-spec replace(string(),string(), string()) -> string().
replace(Original, ToReplace, ReplaceWith) ->
    Index = string:str(Original,ToReplace),
    Len = length(ToReplace),
    LeftSide = string:substr(Original,1,Index-1),
    RightSide = string:substr(Original,Index+Len),
    lists:append([LeftSide,ReplaceWith,RightSide]).

calculate(Series)->
    lists:map(fun(X) ->
		      babel_calc:eval(X)
	      end, Series).

-spec replace_tokens_with_values(string(), [list()]) -> [string()].					
replace_tokens_with_values(Algebra,List) ->
    Tokens = string:tokens(Algebra,"()+-/*^"),
    Transposed = transpose(List),    
    R = lists:map(fun(X) ->
			  lists:foldl(fun(Y,Acc) ->
					      {A,Counter} = Acc,
					      Token = lists:nth(Counter,Tokens),
					      Replaced = replace_token_with_value(A,Token,[Y]),
					      {Replaced,Counter+1}
				      end,{Algebra,1},X)			
		  end,Transposed),
    [Result || {Result,_} <- R].

-spec replace_token_with_value(string(), string(), number()) -> string().				       
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


to_babelstat_records(Docs) ->
    lists:map(fun(X) ->
		      to_babelstat_record(X)
	      end,Docs).

to_babelstat_record({[{<<"_id">>,Id},{<<"_rev">>,Rev},{<<"date">>,Date},{<<"value">>, Value},
		     {<<"metric">>, Metric}, {<<"scale">>, Scale}, {<<"frequency">>,Frequency},
		     {<<"location">>,Location}, {<<"category">>,Cat}, {<<"sub_category">>,SubCat},
		     {<<"subject">>,Subject}, {<<"series_category">>,SerCat}, {<<"title">>,Title},
		     {<<"source">>,Source}, {<<"calculation">>,Calculation}, {<<"constant">>,Constant}]}) ->
    #babelstat{id = Id, rev = Rev, date = binary_to_list(Date), value = Value, 
	       metric = binary_to_list(Metric), scale = Scale, 
	       frequency = binary_to_atom(Frequency,latin1),
	       location = binary_to_list(Location), category = binary_to_list(Cat), 
	       sub_category = binary_to_list(SubCat), subject = binary_to_list(Subject), 
	       series_category = binary_to_list(SerCat), title = binary_to_list(Title), source = Source,
	       calculation = Calculation, constant = Constant}.

test_query() ->
    Params = ["Spawnfest","Teams","Jesus don't want me for a sunBEAM","code","number of lines"],
    Filter = {"unit", 1, daily, "2000-01-01", "2000-01-20"},    
    View = to_babelstat_records(document_creator:get_docs()),
    case View of
	[Doc|[]] ->
	    %%Single document returned, either it is a constant or a calculation
	    case Doc#babelstat.constant of
		true ->
		    create_constants_series(Params, Filter,Doc#babelstat.value,Doc#babelstat.scale,Doc#babelstat.metric);
		false ->
		    %%it's a calculation
		    Calc = Doc#babelstat.calculation,
		    {Queries, Algebra} = parse_calculation(Calc),
		    Series = lists:map(fun(Serie) ->
					       query_db(Serie,Filter)
				       end,Queries),
		    calculate(replace_tokens_with_values(Series,Algebra))			
	    end;
	[_H|_T] = Docs ->
	    {Dates,Values} = lists:foldl(fun(Doc, Acc) ->
						 {Dates, Values} = Acc,
						 {Dates++[Doc#babelstat.date],Values++[Doc#babelstat.value]}     
					 end,{[],[]},Docs),
	    convert_docs_to_series(Params, Filter, {Values,Dates}, Docs);
	_ ->
	    {error, no_documents_found}
    end.

test_query(Measurement,Scale, Frequency) ->
    Params = ["Spawnfest","Teams","Jesus don't want me for a sunBEAM","code","number of lines"],
    Filter = {"cm", 1, daily, "2000-01-01", "2002-01-01"},    
    View = to_babelstat_records(document_creator:get_docs(Measurement, Scale,Frequency)),
    case View of
	[Doc|[]] ->
	    %%Single document returned, either it is a constant or a calculation
	    case Doc#babelstat.constant of
		true ->
		    create_constants_series(Params, Filter,Doc#babelstat.value,Doc#babelstat.scale,Doc#babelstat.metric);
		false ->
		    %%it's a calculation
		    Calc = Doc#babelstat.calculation,
		    {Queries, Algebra} = parse_calculation(Calc),
		    Series = lists:map(fun(Serie) ->
					       query_db(Serie,Filter)
				       end,Queries),
		    calculate(replace_tokens_with_values(Series,Algebra))			
	    end;
	[_H|_T] = Docs ->
	    {Dates,Values} = lists:foldl(fun(Doc, Acc) ->
						 {Dates, Values} = Acc,
						 {Dates++[Doc#babelstat.date],Values++[Doc#babelstat.value]}     
					 end,{[],[]},Docs),
	    convert_docs_to_series(Params, Filter, {Values,Dates}, Docs);
	_ ->
	    {error, no_documents_found}
    end.

test_dates() ->
    Docs = document_creator:get_docs(<<"cm">>,1,<<"daily">>),
    Records = to_babelstat_records(Docs),
    [{Values, Dates}] = lists:map(fun(X) -> {_,D,V,_,_,_,_,_,_,_,_,_,_,_} = X, {V,D} end,[babel_calc:test_query(<<"feet">>,100,<<"daily">>)]),
    babel_calc:date_adjust(Values,Dates,weeks, Records,hd(Dates),lists:nth(length(Dates),Dates)).

    

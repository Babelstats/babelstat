fun(Doc, {Req}) ->  
%%  Out = "hello, world!",
	{Q} = proplists:get_value(<<"query">>,Req),
	error_logger:info_msg("Request is ~p~n",[Q]),
	Body = {<<"response">>, [
				{<<"hola">>, <<"mundo">>},
				{<<"uno">>,<<"dos">>}
			       ]},
	%%Get the query params and create the query record
	[[],Category,[]] = re:replace(proplists:get_value(<<"category">>,Q),<<"\"">>,<<"">>,[global]),
	[[],SubCategory,[]] = re:replace(proplists:get_value(<<"sub_category">>,Q),<<"\"">>,<<"">>,[global]),
	[[],Subject,[]] = re:replace(proplists:get_value(<<"subject">>,Q),<<"\"">>,<<"">>,[global]),
	[[],SeriesCategory,[]] = re:replace(proplists:get_value(<<"series_category">>,Q),<<"\"">>,<<"">>,[global]),
	[[],Title,[]] = re:replace(proplists:get_value(<<"title">>,Q),<<"\"">>,<<"">>,[global]),

	Query = babelstat_api:create_query(Category, SubCategory, Subject, SeriesCategory, Title),
	error_logger:info_msg("Query built ~p~n",[Query]),
	%%Get the filter params and create the filter record
	[[],Metric,[]] = re:replace(proplists:get_value(<<"metric">>,Q),<<"\"">>,<<"">>,[global]),
	[[],Scale,[]] = re:replace(proplists:get_value(<<"scale">>,Q),<<"\"">>,<<"">>,[global]),
	[[],Frequency,[]] = re:replace(proplists:get_value(<<"frequency">>,Q),<<"\"">>,<<"">>,[global]),
	From = proplists:get_value(<<"from_date">>,Q,undefined),
	To = proplists:get_value(<<"to_date">>,Q,undefined),
	Scale0 = list_to_integer(binary_to_list(Scale)),
	Frequency0 = list_to_atom(binary_to_list(Frequency)),
	error_logger:info_msg("New scale ~p, New frequency ~p~n",[Scale0,Frequency0]),
	
	Filter = babelstat_api:create_filter(Metric,Scale0,Frequency0,From,To),
	error_logger:info_msg("Filter done ~p~n",[Filter]),
	Pid = self(),
	
	babelstat_api:run_query(Query,Filter,fun(Res) -> 	
						     error_logger:info_msg("Babelstat returned ~p~n",[Res]),
						     {result, Series} = Res,
						     Json = {babelstat_api:result_to_proplist(Series)},
						     error_logger:info_msg("Babelstat converted to ~p~n",[Json]),
						     Pid ! Json end),
	receive
	    Data ->
		error_logger:info_msg("Returning from show: ~p~n",[Data]),
		{[
		  {<<"headers">>,
		   {
		     [
		      {<<"Content-Type">>,<<"application/json">>}
		     ]
		   }
		  },
		  {<<"json">>,Data}
		 ]}
	end 
end.

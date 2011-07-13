fun({Doc}) ->
	case proplists:get_value(<<"type">>, Doc, null) of
	    null ->
		undefined;
	    _ ->
		Created = proplists:get_value(<<"created_date">>, Doc),
		Emit(Created, {Doc})
	end
end.

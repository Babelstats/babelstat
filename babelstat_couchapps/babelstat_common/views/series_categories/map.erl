fun({Doc}) ->
        case proplists:get_value(<<"type">>, Doc, null) of
            null ->
                undefined;
            _ ->
                Category = proplists:get_value(<<"category">>, Doc),
		SubCategory = proplists:get_value(<<"sub_category">>, Doc),
		Subject = proplists:get_value(<<"subject">>, Doc),
		SeriesCategory = proplists:get_value(<<"series_category">>, Doc),
                Emit([Category,SubCategory,Subject],SeriesCategory)
        end
end.


-module(babelstat_api).
-include("../include/babelstat.hrl").
-export([create_filter/5,
	 create_query/5,
	 run_query/3,
	 result_to_proplist/1]).

-spec create_filter(Metric::binary(),
		    Scale::integer(),
		    Frequency::frequency(),
		    FromDate::babel_date() | undefined,
		    ToDate::babel_date() | undefined) ->
			   #babelstat_filter{} | invalid_filter.
create_filter(Metric, Scale, Frequency, FromDate, ToDate) when is_binary(Metric),
							       is_integer(Scale),
							       is_atom(Frequency) ->
    #babelstat_filter{ metric = Metric,
		       scale = Scale,
		       frequency = Frequency,
		       from_date = FromDate,
		       to_date = ToDate}.

-spec create_query(Category::binary(), SubCategory::binary(), Subject::binary(),
		   SeriesCategory::binary(), Title::binary()) ->
			  #babelstat_query{} | invalid_query.
create_query(Category, SubCategory, Subject, SeriesCategory, Title) ->
    #babelstat_query{ category = Category,
		      sub_category = SubCategory,
		      subject = Subject,
		      series_category = SeriesCategory,
		      title = Title}.

-spec run_query(Query::#babelstat_query{}, Filter::#babelstat_filter{},
		Callback::fun()) ->
		       {ok, Pid::pid()}.
run_query(Query, Filter, Callback) ->
    babelstat_calculation_sup:add_child(Query, Filter, Callback).

-spec result_to_proplist(#babelstat_series{}) ->
				[term()].
result_to_proplist(#babelstat_series{ series = Series,
				      metric = Metric,
				      scale = Scale,
				      frequency = Frequency,
				      location = Location,
				      category = Category,
				      sub_category = SubCategory,
				      subject = Subject,
				      series_category = SeriesCategory,
				      title = Title,
				      source = Source,
				      legend = Legend }) ->
    {Dates, Values} = lists:foldl(fun({Date,Value},Acc) ->
					  {[Dates],[Values]} = Acc,
					  {Dates++[Date],Values++[Value]}
				  end,{[],[]},Series),
    [{dates, Dates},
     {values, Values},
     {metric, Metric},
     {scale, Scale},
     {frequency, Frequency},
     {location, Location},
     {category, Category},
     {sub_category, SubCategory},
     {subject, Subject},
     {series_category, SeriesCategory},
     {title, Title},
     {source, Source},
     {legend, Legend}];

result_to_proplist(no_results) ->
    {[{<<"result">>,<<"no_results">>}]};
result_to_proplist(_) ->
    {[{<<"result">>,<<"unknown_results">>}]}.



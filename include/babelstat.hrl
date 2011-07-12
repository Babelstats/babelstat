-export_type([geo/0, frequency/0, db_result/0, babel_date/0]).
-type geo() :: [{float(),float()}].
-type frequency() :: seconds | minutes | hours | days | weeks | months | years.
-type db_result() :: [{binary() , float() | integer()| atom | list()}].
-type babel_date() ::  calendar:t_datetime1970().
%%The document as it is in the database
-record(babelstat,
	{
	  id :: binary(),
	  rev :: binary(),
	  type :: atom | babelstat,
	  date :: calendar:t_datetime1970(),
	  value :: float(),
	  metric :: binary(),
	  scale :: integer(),
	  frequency :: frequency(),
	  location :: geo(),
	  category :: binary(),
	  sub_category :: binary(),
	  subject :: binary(),
	  series_category :: binary(),
	  title :: binary(),
	  source :: binary(),
	  calculation :: binary(),
	  constant :: boolean(),
	  created_date :: calendar:t_datetime1970()
	}).

-record(babelstat_query,
	{
	  category :: binary(),
	  sub_category :: binary(),
	  subject :: binary(),
	  series_category :: binary(),
	  title :: binary()
	}).

-record(babelstat_filter,
	{
	  metric :: binary() | atom,
	  scale :: integer(),
	  frequency :: frequency(),
	  from_date :: calendar:t_datetime1970(),
	  to_date :: calendar:t_datetime1970()
	}).

%%The output from shows
-record(babelstat_series,
	{
	  series :: [{calendar:t_datetime1970(),float()}],
	  metric :: binary(),
	  scale :: integer(),
	  frequency :: frequency(),
	  location :: geo() | undefined,
	  category :: binary(),
	  sub_category :: binary(),
	  subject :: binary(),
	  series_category :: binary(),
	  title :: binary(),
	  source :: binary(),
	  legend :: binary()
	}).

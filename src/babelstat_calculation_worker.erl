-module(babelstat_calculation_worker).

-behaviour(gen_fsm).

-include("../include/babelstat.hrl").

%% API
-export([start_link/3]).

%% gen_fsm callbacks
-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
	 terminate/3, code_change/4]).

%% gen_fsm states
-export([waiting_for_workers/2,
	 doing_calculation/2]).

-define(SERVER, ?MODULE).
-define(DB_MODULE, babelstat_couchdb).

-record(state, {
	  callback :: fun(),
	  search_query :: #babelstat_query{},
	  filter :: #babelstat_filter{},
	  workers :: integer(),
	  algebra = undefined :: string() | undefined,
	  result :: any()
	 }).

%%%===================================================================
%%% API
%%%===================================================================
-spec start_link(Query::#babelstat_query{},
		 Filter::#babelstat_filter{}, Callback::fun()) ->
			{ok, Pid::pid()}.
start_link(Query, Filter, Callback) ->
    gen_fsm:start_link(?MODULE, [Query, Filter, Callback], []).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================
init([SearchQuery, Filter, Callback]) ->
    process_flag(trap_exit, true),
    gen_fsm:send_event(self(), start),
    {ok, doing_calculation, #state {
	   callback = Callback,
	   filter = Filter,
	   search_query = SearchQuery
	  }}.

-spec doing_calculation(start, #state{}) ->
			       {stop, normal, #state{}} |
			       {next_state, waiting_for_workers, #state{}} |
			       {stop, error, #state{}}.
doing_calculation(start, #state {
		    filter = Filter,
		    search_query = SearchQuery} = State) ->
    case ?DB_MODULE:query_database(SearchQuery, Filter) of
	{ok, [#babelstat{calculation = Calc} = Result|[]]} ->
	    % Single result
	    case {Result#babelstat.constant =:= true, is_binary(Result#babelstat.calculation)} of
		{true, _} ->
		    % It's a constant
		    Series = babelstat_utils:create_constants_series(SearchQuery, Filter, Result#babelstat.value,
								      Result#babelstat.scale, Result#babelstat.metric),
		    {stop, normal, State#state{ result = {result, Series} }};
		{_, true} ->
		    % It's a calculation
		    {Queries, Algebra} = babelstat_utils:parse_calculation(Calc),
		    Self = self(),
		    Workers = length(lists:map(fun(Serie) ->
						       babelstat_calculation_worker:start_link(Serie, Filter,
											       fun(Res) ->
												       gen_fsm:send_event(Self, Res)
											       end)
					end, Queries)),
		    {next_state, waiting_for_workers, State#state{ workers = Workers,
								   algebra = Algebra,
								   result = [] }};
		{_, _} ->
		    % This is a single document
		    {stop, normal, State#state{ result = {result, Result} }}
	    end;
	{ok, Results} ->
	    {Dates,Values} = lists:foldl(fun(Doc, Acc) ->
						 {Dates, Values} = Acc,
						 {Dates++[Doc#babelstat.date],Values++[Doc#babelstat.value]}     
					 end,{[],[]},Results),
	    Results1 = babelstat_utils:convert_docs_to_series(SearchQuery, Filter, {Values,Dates}, Results),
	    {stop, normal, State#state{ result = {result, Results1#babelstat_series{legend = babelstat_utils:create_legend(SearchQuery,Filter)}} }};
	no_results ->
	    {stop, error,  State#state{ result = {error, no_document_found} }}
    end.

-spec waiting_for_workers({result, Results::#babelstat_series{}} |
			  {error, no_document_found}, #state{}) ->
				 {stop, normal, #state{}} |
				 {stop, error, #state{}} |
				 {next_state, waiting_for_workers, #state{}}.
waiting_for_workers({error, Error},  State) ->
    {stop, error, State#state{ result = {error, Error}}};

waiting_for_workers({result, NewResults}, #state{result = Results,
						 workers = 1,
						 algebra = Algebra,
						 search_query = #babelstat_query{ category = Category,
										  subject = Subject,
										  series_category = SeriesCategory,
										  title = Title } = Q,
						 filter = #babelstat_filter{ metric = Metric,
									     scale = Scale,
									     frequency = Frequency} = F
						} = State) ->
    CalculatedResults = babel_calc:calculate(babelstat_utils:replace_tokens_with_values(Algebra, Results++[NewResults])),
    Results1 = NewResults#babelstat_series{category = Category,
					   subject = Subject,
					   series_category = SeriesCategory,
					   title = Title,
					   metric = Metric,
					   scale = Scale,
					   frequency = Frequency,
					   series = CalculatedResults,
					   legend = babelstat_utils:create_legend(Q,F)},
	{stop, normal, State#state{ result = {result, Results1},
				    workers = 0 }};

waiting_for_workers({result, NewResult}, #state{ result = Result, 
					       workers = Workers} = State) ->
    {next_state, waiting_for_workers, State#state{ result = Result ++ [NewResult],
						   workers = Workers - 1}}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(error, _, #state { result = Result, callback = Callback }) ->
    Callback(Result);

terminate(normal, _, #state{ result = Result, callback = Callback }) ->
    Callback(Result);

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

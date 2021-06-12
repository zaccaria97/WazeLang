-module(secondary_node).

-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3,handle_cast/2, handle_info/2, terminate/2]).
-export([get_new_primary_node_id/2,get_new_primary_node_id/3, add_to_cluster/1]).


-include("utility.hrl").

-define(SERVER, ?MODULE).

  
% (?) Qui l'utente non deve passare come argomento Primary_node ma in realtà occorre che venga recuperato direttaemente
% 	  andando ad interagire con il server di Raffaele
start_link(Primary_node) ->
	% Start link arguments:
	% - {local, ?SERVER} -> register the server locally associating the name via MACRO
	% - The second argument is the name of the callback module where the callback functions are located, in this case is
	%   is the current module
	% - Primary_node is passed as argument to the callback function init
  gen_server:start_link({local, ?SERVER}, ?MODULE, Primary_node, []).

init(Primary_node) ->
	% During the initialization phase the node must ask to the primary to add itself to the cluster of secondary nodes
  Neighbours_list = add_to_cluster(Primary_node),
  io:format("List ~w~n", [Neighbours_list]),
	% Start a periodic timer exploiting the send_after function
  erlang:send_after(?TIMEOUT_ALIVE, secondary_node, {ping_pong}), %%handled by handle_info callback
  Now = erlang:monotonic_time(millisecond),
	% The state of the server is initialized and it contains:
	% - The list of neighbours as provided by the primary node
	% - A reference to the Primary_node as provided by the user
	% - Now (?) Perchè
  {ok,{Neighbours_list, Primary_node, Now}}.  
 

handle_cast(Request, {Neigh_list, Primary_node, Primary_last_contact}) ->
	% Whenever the gen_server process receive a request sent using cast/2 this function is called to
	% handle such request
  case Request of
  	{heartbeat,From} when is_atom(From) ->
			% Case is an heartbeat message received from the primary node
      io:format("[secondary node] received a heartbeat mex from primary node: ~w~n", [From]), % DEBUG
	  	Now = erlang:monotonic_time(millisecond),
			% The secondary update its state, in particular the last time contact is updated with the current
			% instant
      {noreply, {Neigh_list, Primary_node, Now}};

		_ ->
      io:format("[secondary_node] WARNING: bad request format~n"),
      {noreply, {Neigh_list, Primary_node, Primary_last_contact}}
  end. 
 
% (?)
handle_call(stop, From, {Neigh_list, Primary_node, Primary_last_contact}) ->
    {stop, normal, shutdown_ok, {Neigh_list, Primary_node, Primary_last_contact}};
 
handle_call(Request, From, {Neigh_list, Primary_node, Primary_last_contact}) ->
	%Synchronous request
	% This function is called whenever a gen_server process receives a request sent using call, this
	% function is called to handle such request.
  io:format("Call requested: Request = ~w From = ~w ~n",[Request, From]), % DEBUG
  case Request of
    {neighbour_add_propagation, New_Neigh} ->
      io:format("[secondary node] has received a new neighbour: ~w~n", [New_Neigh]),
	  	if
				Neigh_list == [] ->
					New_Neigh_list = [New_Neigh];
				true ->
					New_Neigh_list = Neigh_list ++ [New_Neigh]
	  	end,
			% (?) Qui non va aggiornato anche il Primary_last_contact
			% The handle_call must return {reply,Reply,NewState} so that the Reply will be given back to From
			% as the return value of call, in this case the atom update_neighbours_reply is returned.
			% The gen_server process then continues executing updating its state with just arrived node
      {reply, update_neighbours_reply, {New_Neigh_list, Primary_node, Primary_last_contact}};

    {neighbour_del_propagation, Neigh} ->
			io:format("[secondary node] must delete the node from the neighbours: ~w~n", [Neigh]),  %%DEBUG
			if
				is_list(Neigh) ->
					% filter(Pred, Neigh_list) returns a list of all the elements that belongs to Neigh_list that
					% for which Pred(element) == true. In this case in particular it returns the list of neighbours
					% that does not belong to the list - of nodes to delete - passed within the message
					New_Neigh_list=lists:filter(fun (Elem) -> not lists:member(Elem, Neigh) end, Neigh_list );
				true ->
					New_Neigh_list=lists:filter(fun (Elem) -> not lists:member(Elem, [Neigh]) end, Neigh_list )
			end,
			{reply,update_neighbours_reply,{New_Neigh_list, Primary_node, Primary_last_contact}};

		{add_point, Body}  ->
			% (?) Code che cos'è
			io:format("[secondary node] must add the following point:  ~p~n", [Body]),  %%DEBUG
	  	{Code,Geog}=Body,
	  	Query="INSERT INTO positions VALUES ('" ++ Code ++ "', '" ++ Geog ++ "');",
	  	odbc:start(),
	  	{ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgres;UID=postgres;PWD=admin",[]),
	  	Result=odbc:sql_query(Ref, Query),
			{reply,{add_point_reply,Result},{Neigh_list, Primary_node, Primary_last_contact}};

    %% catch all clause
    _ ->
      io:format("[secondary_node] WARNING: bad request format ~n"),
      {reply, bad_request, {Neigh_list, Primary_node, Primary_last_contact}}
  end.
  
  
handle_info(Info, {Neigh_list, Primary_node, Primary_last_contact}) ->
	% The handle_info callback is for messages that don’t originate from the functions call and cast of
	% the gen_server module. In our case is used for the timer implementation via send_after.
  case Info of
		{ping_pong} ->
			% In this case the secondary check if the primary is still alive i.e. the primary has contacted
			% the secondary in the last 2 * TIMEOUT milliseconds
			Result = check_alives([{Primary_node,Primary_last_contact}]),
	  	case Result == [] of
				false ->
					% Case primary is still alive
					io:format("[secondary node] Primary node is still alive... ~n"), % DEBUG
					io:format("----------NEIGHBOURS---------~n"),
					% Print the neighbours information if any
					case Neigh_list == [] of
						false ->
							lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Neigh_list), %DEBUG
							io:format("~n");
						true ->
							io:format("There are no secondary nodes...~n~n")
					end,
					% Send to the primary node the heartbeat message
					gen_server:cast({primary_node, Primary_node}, {heartbeat,node()}),
					% Restart the timer
					erlang:send_after(?TIMEOUT_ALIVE, secondary_node, {ping_pong}),
					{noreply,{Neigh_list, Primary_node, Primary_last_contact}};

				true ->
					% Case the primary has failed => election mechanism must start
					io:format("[secondary node] primary node has failed! Election mechanism is started... ~n"), % DEBUG
					Candidates_list = Neigh_list ++ [node()],
					io:format("@@@@@Candidates_list ~w~n",[Candidates_list]), % DEBUG
					% atom_to_list return the text representation of the atom Node_name
					% substr(String, Start, Length) returns a substring of String that starts at position Start
					% The node name format that we expect is nodexx@localhost where xx are two digits
					Nodes_id = [string:substr(atom_to_list(Node_name),5,2) || Node_name <- Candidates_list],
					% list_to_integer(String) returns an integer whose text representation is String
					% then this list is sorted.
					Sorted_nodes_id = lists:sort([list_to_integer(String_id) || String_id <- Nodes_id]),
					Y = list_to_integer(string:substr(atom_to_list(Primary_node),5,2)),
					io:format("@@@@@ ~w    ~w     ~w ~n",[Y, Sorted_nodes_id, length(Sorted_nodes_id)]), % DEBUG
					% (?)
					New_Primary_node_id = get_new_primary_node_id(Y, Sorted_nodes_id),
					io:format("----- ~w  ~w     ~w~n",[Sorted_nodes_id, New_Primary_node_id, Candidates_list]), % DEBUG
					% lists:nth(N,List) returns the Nth element of the list, in this case is used in order to obtain the
					% name of the node whose identifier was obtained in the previous steps
					New_Primary_node_name = lists:nth(1,[Node_name || Node_name <- Candidates_list,
													list_to_integer(string:substr(atom_to_list(Node_name),5,2)) == New_Primary_node_id]),
					io:format("CANDIDATES: ~w ~n",[Candidates_list]), % DEBUG
					io:format("NEW PRIMARY ~w ~n",[New_Primary_node_name]), % DEBUG
					if
						New_Primary_node_name == node() ->
							% Case this secondary node is the one that has to began the new primary
							io:format("[secondary node] Turning to primary node.... ~n"), % DEBUG
							% In Erlang a new process is created by calling the function spawn, in this case the new process
							% start executing the function elect of the Module primary_node, we pass to this function
							% no argument
							spawn(primary_node, elect, []),
							% (?) Ma qui il nodo secondary non va terminato
							{noreply,{[], New_Primary_node_name, null}};
						true ->
							io:format("[secondary node] New primary node is ~w ~n",[New_Primary_node_name]), % DEBUG
							% (?) Ma qui non avevamo detto che non veniva fatto l'add_to_cluster
							Neighbours_list = add_to_cluster(New_Primary_node_name),
							% The timer is restarted
							erlang:send_after(?TIMEOUT_ALIVE, secondary_node, {ping_pong}),
							Now = erlang:monotonic_time(millisecond),
							{noreply, {[], New_Primary_node_name, Now}}
					end
			end;

		_Dummy ->
      io:format("[secondary node] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy]), % DEBUG
      {noreply, {Neigh_list, Primary_node, Primary_last_contact}}
  end.

get_new_primary_node_id(N, [H|T]) ->
	if 
		N < H ->
			H;
		  
		true ->
			get_new_primary_node_id(N, T, [H])
	end.
  
get_new_primary_node_id(N, [], [H|T]) ->
	H;
  
get_new_primary_node_id(N, [H|T], L) ->
	if 
		N < H ->
		  
			H;
		  
		true ->
			
			get_new_primary_node_id(N, T, L ++ [H])
	end.

  
check_alives(Neigh_list) ->
	% This function return the nodes from the list passed as argument that contacts this node in the last
	% 2 * TIMEOUT_ALIVE milliseconds
  Now = erlang:monotonic_time(millisecond),
  [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Neigh_list,
                                  Now - Last_time_contact < 2 * ?TIMEOUT_ALIVE].
  
% (?) Qui perchè non c'è il timeout
add_to_cluster(Node) ->
	% Via gen_server:call that makes a synchronous request to the process with Name = primary_node and located on the node
	% passed as argument(in this case the primary node provided by the command line). This function returns the list of
	% neighbours as provided by the primary node.
  gen_server:call({primary_node, Node}, {neighbour_add, node()}).
								  
	

get_diff([],L2) ->
	[];
	
get_diff([H|T],L2) ->
	if 
		is_tuple(H) ->
			
			Name = element(1,H);
			
		true ->
			Name = H
		end,
	io:format("  L2 :  ~w~n", [L2]),
	io:format("  H , NAME :  ~p , ~p~n", [H, Name]),
	case lists:member(Name, L2) of
		true ->
			get_diff(T,L2);
		false ->
			[H] ++ get_diff(T,L2)
	end.
	
	
terminate(_Reason, _State) ->
  ok.
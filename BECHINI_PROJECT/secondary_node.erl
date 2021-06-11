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
 

handle_cast(Request, {Neigh_list, Primary_node, Primary_last_contact}) -> %Asynchronous request
  % format_state(State), % DEBUG
  case Request of
  
    {heartbeat,From} when is_atom(From) -> %%and From == Primary_node ->
      io:format("[secondary node] received a heartbeat mex from primary node: ~w~n", [From]), % DEBUG
	  Now = erlang:monotonic_time(millisecond),
      {
        noreply,
        {Neigh_list, Primary_node, Now}
      };

    %% catch all clause
    _ ->
      io:format("[secondary_node] WARNING: bad request format~n"),
      {noreply, {Neigh_list, Primary_node, Primary_last_contact}}

  end. 
 
 
handle_call(stop, From, {Neigh_list, Primary_node, Primary_last_contact}) ->
    {stop, normal, shutdown_ok, {Neigh_list, Primary_node, Primary_last_contact}};
 
handle_call(Request, From, {Neigh_list, Primary_node, Primary_last_contact}) -> %Synchronous request
  io:format("Call requested: Request = ~w From = ~w ~n",[Request, From]), % DEBUG
  %format_state(State), % DEBUG
  case Request of
	
	%%IL SECONDARIO RICEVE IL NODO DA AGGIUNGERE AI VICINI DAL PRIMARIO
    {neighbour_add_propagation, New_Neigh} ->
      %io:format("time : ~w" ,[erlang:monotonic_time(millisecond)]),
      io:format("[secondary node] has received a new neighbour: ~w~n", [New_Neigh]),
	  if
		Neigh_list == [] ->
		
		  New_Neigh_list = [New_Neigh];
		  
		true ->
		
		  New_Neigh_list = Neigh_list ++ [New_Neigh]
	  end,
      {
        reply,
		update_neighbours_reply,
        {New_Neigh_list, Primary_node, Primary_last_contact}
      };

    {neighbour_del_propagation, Neigh} ->
		%io:format("time : ~w" ,[erlang:monotonic_time(millisecond)]),
	  
		io:format("[secondary node] must delete the node from the neighbours: ~w~n", [Neigh]),  %%DEBUG
		if is_list(Neigh) ->
			New_Neigh_list=lists:filter(fun (Elem) -> not lists:member(Elem, Neigh) end, Neigh_list );
		true ->
			New_Neigh_list=lists:filter(fun (Elem) -> not lists:member(Elem, [Neigh]) end, Neigh_list )
		end,

		
	  %%New_Neigh_list = get_diff(Neigh_list, [Neigh]),
	  {
        reply,
		update_neighbours_reply,
        {New_Neigh_list, Primary_node, Primary_last_contact}
      };

	{add_point, Body}  ->
	  io:format("[secondary node] must add the following point:  ~p~n", [Body]),  %%DEBUG
	  {Code,Geog}=Body,
	  Query="INSERT INTO positions VALUES ('" ++ Code ++ "', '" ++ Geog ++ "');",
	  odbc:start(),
	  {ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgres;UID=postgres;PWD=admin",[]),
	  Result=odbc:sql_query(Ref, Query),
	  
      {
        reply,
        {add_point_reply,Result},
        {Neigh_list, Primary_node, Primary_last_contact}
      };

    %% catch all clause
    _ ->
      io:format("[secondary_node] WARNING: bad request format ~n"),
      {reply, bad_request, {Neigh_list, Primary_node, Primary_last_contact}}
  end.
  
  
handle_info(Info, {Neigh_list, Primary_node, Primary_last_contact}) ->
	% The handle_info callback is for messages that don’t originate from the functions call and cast of the gen_server
	% module. In our case is used for the timer implementation via send_after.
  case Info of
		{ping_pong} ->
      Result = check_alives([{Primary_node,Primary_last_contact}]),
	  case Result == [] of
		false -> 
			io:format("[secondary node] Primary node is still alive... ~n"), % DEBUG
			io:format("----------NEIGHBOURS---------~n"),
			case Neigh_list == [] of
				false -> 
					lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Neigh_list), %DEBUG
					io:format("~n")
					;
				true ->
				
					io:format("There are no secondary nodes...~n~n")
				end,

			gen_server:cast({primary_node, Primary_node}, {heartbeat,node()}),
			erlang:send_after(?TIMEOUT_ALIVE, secondary_node, {ping_pong}),
			{
				noreply,
				{Neigh_list, Primary_node, Primary_last_contact}
			};
		true ->
			
		    io:format("[secondary node] primary node has failed! Election mechanism is started... ~n"), % DEBUG
		
			Candidates_list = Neigh_list ++ [node()],
			io:format("@@@@@Candidates_list ~w~n",[Candidates_list]), % DEBUG
			
			Nodes_id=[string:substr(atom_to_list(Node_name),5,2) || Node_name <- Candidates_list],
			
			
			Sorted_nodes_id= lists:sort([list_to_integer(String_id) || String_id <- Nodes_id]),
						
			Y=list_to_integer(string:substr(atom_to_list(Primary_node),5,2)),
			
			io:format("@@@@@ ~w    ~w     ~w ~n",[Y, Sorted_nodes_id, length(Sorted_nodes_id)]), % DEBUG
			
			New_Primary_node_id=get_new_primary_node_id(Y, Sorted_nodes_id),
			
			io:format("----- ~w  ~w     ~w~n",[Sorted_nodes_id, New_Primary_node_id, Candidates_list]), % DEBUG

			New_Primary_node_name=lists:nth(1,[Node_name || Node_name <- Candidates_list,
													list_to_integer(string:substr(atom_to_list(Node_name),5,2)) == New_Primary_node_id]),
													
			io:format("CANDIDATES: ~w ~n",[Candidates_list]), % DEBUG
			io:format("NEW PRIMARY ~w ~n",[New_Primary_node_name]), % DEBUG
			if 
				New_Primary_node_name == node() ->
					io:format("[secondary node] Turning to primary node.... ~n"), % DEBUG
					spawn(primary_node, elect, []),
					{
						noreply,
						{[], New_Primary_node_name, null}
					};
				  
				true ->
					io:format("[secondary node] New primary node is ~w ~n",[New_Primary_node_name]), % DEBUG

				%%	gen_server:cast({New_Primary_node_name, Primary_node}, {heartbeat,node()}),

					Neighbours_list=add_to_cluster(New_Primary_node_name),
					
					erlang:send_after(?TIMEOUT_ALIVE, secondary_node, {ping_pong}),
					%%New_Neigh_list = lists:delete(New_Primary_node_name, Neigh_list),
					Now = erlang:monotonic_time(millisecond),
					{
						noreply,
						{[], New_Primary_node_name, Now}
					}
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
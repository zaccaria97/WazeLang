%%IL MODULO CONTIENE IL PRIMARY_NODE, CHE POSSIEDE UNA COPIA DEL DB.
%%ATTRAVERSO UN ALGORITMO DI ELEZIONE VIENE SCELTO UN RM CHE DEVE LAVORARE COME PRIMARIO. TUTTI I NODI POSSEGGONO
%%UNA LISTA DEGLI ALTRI NODI(neighbours_list). LE RICHIESTE DI AGGIUNTA E CANCELLAZIONE DI UN NODO VENGONO MANDATE
%%AL PRIMARIO, CHE DEVE AGGIORNARE PRIMA LE LISTE DEGLI ALTRI NODI, ATTENDERE GLI ACK E POI AGGIORNARE LA PROPRIA.
%%CIASCUN NODO MANDA UN alive SOLO AL PRIMARIO; SE IL PRIMARIO NON RICEVE UN alive IN TEMPO ALLORA INOLTRA UNA RICHIESTA 
%%DI CANCELLAZIONE A TUTTI I SECONDARI, E ASPETTA L'ACK. 
%%IL PRIMARY RICEVE RICHIESTE CALL DAI NODI CHE SI VOGLIONO AGGREGARE, POI MANDA RICHIESTE CALL BLOCCANTI
%%A TUTTI I SECONDARI ATTRAVERSO SEND_ALL() E POI RISPONDE CON LA LISTA AGGIORNATA.

%%DOMANDA: L'ACK SI IMPLEMENTA ATTRAVERSO LA CALL?
%%DOMANDA: AI SECONDARI MANDARE SOLO IL NODO DA AGGIUNGERE/TOGLIERE O L'INTERA LISTA DEI VICINI?

%%FARE IPOTESI CHE MENTRE IL PRIMARIO INVIA IL NODO DA AGGIUNGERE/TOGLIERE DALLA LISTA DEI VICINI AI SECONDARI NON 
%%POSSA FALLIRE.
%%NELLA LISTA CHE VIENE MANDATA AD UN SECONDARIO NON C'È NÈ IL SECONDARIO STESSO NÈ IL PRIMARIO
%%FARE IN MODO CHE IL NUOVO PRIMARIO CHE RICEVE LA LISTA AGGIORNI IL TIMESTAMP DI TUTTI A NOW()
%%DARE LA POSSIBILITÀ AL PRIMARIO DI ANDARE IN ESECUZIONE CON UNA LISTA DI VICINI GIÀ PRONTA.
%% UN NODO CHE SI DISCONNETTE E POI SI RICONNETTE SUBITO DOPO DA ERRORE.
%%TOGLIERE NEIGHBOUR_DEL
%%USARE LISTS:FILTER AL POSTO DI DIFF
%%RIMUOVERE IL PRIMO HANDLE_CALL
%%LA SEND_ALL ESEGUE UNA CALL PER TUTTI I NODI, E VIENE USATA SOLO PER MANDARE ADD/DEL_PROPAGATION E PER LE UPDATE:
%%GESTIRE I CASI DI ERRORE CON UN UNICO ATOM
%% SE MANDO UPDATE AD UN NODO E QUEL NODO NON LA ESEGUE COME COMPORTARSI? CANCELLARE IL NODO E RIPRISTINARLO CON LO STATO
%%AGGIORNATO.
%%IL PRIMARIO DEVE AGGIORNARE IL PROPRIO DB DOPO AVER AGGIORNATO QUELLO DEGLI ALTRI, o VA BENE ANCHE PRIMA?
%%AGGIUGNERE TIMEOUT A TUTTE LE CALL
%%MECCANISMO ELEZIONE:i nodi potrebbero avere stati inconsistenti (sia la lista dei vicini che lo stato del DB).
%% -1    il primario può mandare i vari update(nuovi vicini o nuovi punti) ogni volta e poi all'elezione il nuovo stato è quello della maggioranza,
%% -2    invece degli update mandare direttamente lo stato per intero
%% -3    una cosa ibrida
%% -4    supporre che durante l'elezione tutti gli stati siano consistenti, e quindi mandare solo gli update.

%%IN JAVA fare una funzione per stabilire il primary node.
%%Verificare che funzioni anche con un solo nodo (il primario) nel cluster
%%erificare cosa succede se si manda una richiesta di update non appena un nodo scondario
%%fallisce
%%CAMBIARE CREDENZIALI DEL DATABASE
%%CREARE MODULO PR INTERAZIONE CON DB
%%COSA SUCCEDE SE DUE NODI SI DISCONNETTONO CONTEMPORANEAMENTE.
%%ALGORITMO ELEZIONE: NELLA FUNZIONE get_new_primary_node_id() FARE IN MODO DI SOMMARE MAX_NODE_NUMEBER AD H.
%%FARE IN MODO DI USARE UN SOLO SEND_AFTER SIA PER VERIFICARE CHE PER MANDARE I PING
%%MODIFICARE LA SELECT IN MODO CHE RESTITUISCA UN FORMATO CORRETTO CON I PUNTI...
%%NEL CODICE FARE DISTINZIONE TRA NODE_ID E NODE_NAME
%%controllare cosa succede quando elimino due nodi contemporaneamente.
-module(primary_node).

-behaviour(gen_server).

-export([add_point/1, get_map/1, start_link/1, init/1, elect/1, elect/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, stop/0]).


-include("utility.hrl").

-define(SERVER, ?MODULE).

-record(rm_state,{neighbours_list}).


get_map({Lat, Lng, Radius}) ->
	Result = gen_server:call(primary_node, {get_map, Lat, Lng, Radius}).

add_point({Type, Lat, Lng, Max_speed}) ->
	gen_server:call(primary_node, {add_point, Type, Lat, Lng, Max_speed}).


start_link(Neighbours_list) ->
  % Start link arguments:
	% - {local, ?SERVER} -> register the server locally associating the name via MACRO
	% - The second argument is the name of the callback module where the callback functions are located, in this case is
	%   is the current module
	% - Neighbours_list is passed as argument to the callback function init
	gen_server:start_link({local, ?SERVER}, ?MODULE, Neighbours_list, []).

init(Neighbours_list) ->
  % If name registration succeeds this function will be called
  io:format("Primary node has been created ~n~n"),
	% send_after start a timer, when the timer expires, the message check_alive and send_heartbeat will be
	% sent to the process called primary node. Note that the usage of send_after is recommended for efficiency
	% purpose.
  erlang:send_after(?TIMEOUT_ALIVE, primary_node, {check_alive}), %%handled by handle_info callback
  erlang:send_after(?TIMEOUT_ALIVE, primary_node, {send_heartbeat}), %%handled by handle_info callback
	% Signal via TCP the information about this primary server
	% update_primary(),
	% The function is expected to return {ok,State} where State is the internal state of the gen_server
  {ok,Neighbours_list}. 
  
elect() ->
	Reply =(catch gen_server:call({secondary_node, node()}, stop, ?CALL_TIMEOUT)),
	io:format("Secondary node has been terminated ~n~n"),
	start_link([]).
  
elect(Node_list) ->
	io:format("CHIAMO ELECT... ~n"), % DEBUG
	gen_server:call({secondary_node, node()}, stop, ?CALL_TIMEOUT),
	io:format("Secondary node has been terminated ~n~n"),
	% Signal via TCP the information about this primary server
	% update_primary(),
	
	
	% Here the latest added point is propagated to all secondary nodes, the result of the broadcast_call
	% is the list of nodes that have failed during the propagation of the check_db_consistency message. 
	Query="SELECT max_speed, type, lat, lng FROM points ORDER BY timestamp DESC LIMIT 1;",
	
	Node_Id = get_node_id(), 
	odbc:start(),
	{ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgis" ++ Node_Id ++ ";UID=postgres;PWD=admin",[]),

	Result=odbc:sql_query(Ref, Query),
	io:format("~p~n", [Result]),  %%DEBUG
	{_,_,L} = Result,
	io:format("~p~n", [L]),  %%DEBUG
	[{Max_speed, Type, Lat, Lng}] = L,
	io:format("~p ~p ~p ~p ~n", [Max_speed, Type, Lat, Lng]),
	
	Failed_Node_List = broadcast_call(Node_list,{check_db_consistency, Max_speed, Type , Lat, Lng}),
	if
		Failed_Node_List == [] ->
			% Case when no nodes failed during the propagation of the check_db_consistency message
			io:format("[primary_node] check_db_consistency operation correctly performed.~n"),
			New_Node_list = Node_list;
		true ->
			% Otherwise the primary get the new list of neighbours
			io:format("[primary_node] Following nodes have failed during check_db_consistency and will be removed: ~w ~n", [Failed_Node_List]),
			New_Node_list= get_diff(Node_list,Failed_Node_List),
			io:format("[primary_node] New secondary nodes list: ~w ~n", [New_Node_list]) %%DEBUG

	end,
					

	Now = erlang:monotonic_time(millisecond),
	New_Neighbours_list = [{X, Now} || X <- New_Node_list],
	io:format("[primary_node]2 New secondary nodes list: ~w ~n", [New_Neighbours_list]),	%%DEBUG
	start_link(New_Neighbours_list).
  
handle_cast(Request, Neigh_list) ->
  % Whenever the gen_server process receive a request sent using cast/2 this function is called to
	% handle such request
  case Request of
    {heartbeat,Neigh} when is_atom(Neigh) ->
	  	New_Neigh_list = update_last_contact(Neigh,Neigh_list),
      {noreply, lists:reverse(lists:keysort(1, New_Neigh_list))};

    %% catch all clause
    _ ->
      io:format("[primary_node] WARNING: bad request format~n"),
      {noreply, Neigh_list}

  end.

handle_call(Request, From, Neigh_list) ->
	% This function is called whenever a gen_server process receives a request sent using call, this
	% function is called to handle such request.
	io:format("Call requested: Request = ~w ~n",[Request]), % DEBUG
	case Request of
    {neighbour_add, New_Neigh} when is_atom(New_Neigh) ->
			% This message is received when a new secondary node wants to join the network, New_Neigh must be an atom
	  	Now = erlang:monotonic_time(millisecond),
	  	if
			Neigh_list == [] ->
				% If there aren't secondary node the primary only adds its to its neighbour list
				New_Neigh_list = [{New_Neigh,Now}],
				% Neigh_list is a list of couple <(?),network_join_instant>, since we have to send an heartbeat message to all
				% the secondary server we take only the list of (?) via list comprehensions
				Returned_Node_list=[X||{X,_} <- Neigh_list],
				io:format("[primary_node] Node ~w has been correctly added.~n", [New_Neigh]);
			true ->
				% Neigh_list is a list of couple <(?),network_join_instant>, since we have to send an heartbeat message to all
				% the secondary server we take only the list of (?) via list comprehensions
				Node_list=[X||{X,_} <- Neigh_list],
				% The function broadcast_call returns the the list of nodes that failed during the neighbour_add_propagation
				% message propagation
				Failed_Node_List = broadcast_call(Node_list,{neighbour_add_propagation,New_Neigh}),
				if
					Failed_Node_List == [] ->
						io:format("[primary_node] neighbors: ~w ~n", [Neigh_list]),
						io:format("[primary_node] Node ~w has been correctly added.~n", [New_Neigh]),
						New_Neigh_list = lists:reverse(lists:keysort(1, Neigh_list ++ [{New_Neigh,Now}])),
						io:format("[primary_node] sorted neighbors: ~w ~n", [New_Neigh_list]),

						% The Returned_Node_list represent the list that is returned to the new node that wants to join the network,
						% this list contains all the secondary nodes that already have joined the network
						Returned_Node_list = Node_list;
					true ->
						io:format("[primary_node] Following nodes have failed and will be removed: ~w ~n", [Failed_Node_List]),
						% The nodes that have failed are removed from the list of neighbour
						New_Neigh_list = lists:reverse(lists:keysort(1, get_diff(Neigh_list ++ [{New_Neigh,Now}],Failed_Node_List))),
						
						% In order to obtain the list of neighbours of the new node this node is subtracted
						% from the new list of neighbours
						Returned_Node_list = [X||{X,_} <- get_diff(New_Neigh_list, [New_Neigh])],
						io:format("[primary_node] New neighbors: ~w ~n", [New_Neigh_list])
			end
		end,
		Query="SELECT * FROM points;",
		Node_Id = get_node_id(), 
		odbc:start(),
		{ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgis" ++ Node_Id ++ ";UID=postgres;PWD=admin",[]),
		{_,_,Result}=odbc:sql_query(Ref, Query),
		io:format("~p~n", [Result]),
		
		% The handle_call must return {reply,Reply,NewState} so that the Reply will be given back to From
		% as the return value of call, in this case Returned_Node_list represent the list of secondary
		% nodes without the nodes that sends the request, and Result is the list of nodes stored in DB. The gen_server process then continues executing
		% updating its state with New_Neigh_list 
		
        {reply, {lists:reverse(lists:sort(Returned_Node_list)), Result}, lists:reverse(lists:keysort(1,New_Neigh_list))};

		{get_map, Lat, Lng, Radius} ->
			% This message is a request to retrieve all the points that belongs to a circle pointed in
			% Lat an Lng and with a radius equal to Radius. Notice that the all the reads, according to
			% primary-secondary paradigm, are served by the primary
			Point="POINT(" ++ Lat ++ " " ++ Lng ++ ")",
			io:format("[primary_node] received a get_map request. Point: ~p, Radius: ~p ~n", [Point, Radius]), 
			
			Query="SELECT * FROM points WHERE ST_Distance(ST_GeographyFromText(CONCAT('POINT(', lat,' ', lng,')')), ST_GeographyFromText('" ++ Point ++ "')) < " ++ Radius ++ ";",
			
			Node_Id = get_node_id(), 
			odbc:start(),
			{ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgis" ++ Node_Id ++ ";UID=postgres;PWD=admin",[]),

			Result=odbc:sql_query(Ref, Query),
			io:format("~p~n", [Result]),
			io:format("[primary_node] GET MAP operation correctly performed.~n"),
			% TODO METTERE CASE OF RESULT E RESTITUIRE SUCCESS/ERROR
			{reply, Result, Neigh_list};
	
		{add_point, Type , Lat, Lng, Max_speed} ->
			% This message is received when a there is the need to add a new point
			io:format("[primary_node] received an add_point request. Type: ~p, Point: POINT( ~p ~p ), Max speed: ~p~n", [Type, Lat, Lng, Max_speed]),
			% First the point is inserted into the primary database
			
			Query="INSERT INTO points(type,lat,lng,max_speed, timestamp) VALUES ('" ++ Type ++ "', '" ++ Lat ++ "', '" ++ Lng ++ "', '" ++ Max_speed ++ "', NOW());",
			Node_Id = get_node_id(), 
			odbc:start(),
			{ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgis" ++ Node_Id ++ ";UID=postgres;PWD=admin",[]),

			Result=odbc:sql_query(Ref, Query),
			% Then the update is propagated to each secondary node(if any)
			if
				Neigh_list =/= [] ->
					% Neigh_list is a list of couple <(?),network_join_instant>, since we have to send an heartbeat message to all
					% the secondary server we take only the list of (?) via list comprehensions
					Node_list=[X||{X,_} <- Neigh_list],
					% Here the update is propagated to all the secondary nodes, the result of the broadcast_call
					% is the list of nodes that have failed during the propagation of the add_point message
					Failed_Node_List = broadcast_call(Node_list,{add_point, Type , Lat, Lng, Max_speed}),
					if
						Failed_Node_List == [] ->
							% Case when no nodes failed during the propagation of the add_point message
							io:format("[primary_node] ADD POINT operation correctly performed.~n"),
							New_Neigh_list = Neigh_list;
						true ->
							% Otherwise the primary get the new list of neighbours
							io:format("[primary_node] Following nodes have failed and will be removed: ~w ~n", [Failed_Node_List]),
							New_Neigh_list= get_diff(Neigh_list,Failed_Node_List)
					end,
					case Result of
						{updated,1} ->
							io:format("[primary node] Primary node has correctly added the point ...~n~n"), % DEBUG
							% The handle_call must return {reply,Reply,NewState} so that the Reply will be given back to From as the return
							% value of call, in this case the primary return the atom success to indicate that all the secondary have
							% updated their database
							{reply, success, lists:reverse(lists:keysort(1,New_Neigh_list))};
						_ ->
							io:format("[primary node] An error occured with primary node during the point addition  ...~n~n"), % DEBUG
							{reply, error, lists:reverse(lists:keysort(1,New_Neigh_list))}
					end;
				true ->
					io:format("[primary node] There are no secondary nodes to forward the ADD POINT operation. ~n~n") % DEBUG					
			end,
			{reply, Result, Neigh_list};

    %% catch all clause
    _ ->
      io:format("[primary node] WARNING: bad request format~n"),
      {reply, bad_request, Neigh_list}
  end.

handle_info(Info, Neigh_list) ->
  % The handle_info callback is for messages that don’t originate from the functions call and cast of the gen_server
	% module. In our case is used for the timer implementation via send_after.
	case Info of
		{send_heartbeat} ->
			% This means that the timer associated with the heartbeat send expires
	  	io:format("------------------------~n"),
      io:format("[primary node] heartbeat is sent to secondary nodes...~n~n"), % DEBUG
	  	io:format("------------------------~n"),
			% Neigh_list is a list of couple <(?),network_join_instant>, since we have to send an heartbeat message to all the
			% secondary server we take only the list of (?) via list comprehensions
			Node_list=[X||{X,_} <- Neigh_list],
			% A message containing the heartbeat atom and the name of the local node
	  	broadcast_cast(Node_list,{heartbeat,node()}),
      % The timer for the send heartbeat task is then restarted
			erlang:send_after(?TIMEOUT_ALIVE, primary_node, {send_heartbeat}),
			% The function must return {noreply,NewState}
      {noreply, Neigh_list};
  
    {check_alive} ->
			% The timer for the check alive task is restarted
      erlang:send_after(?TIMEOUT_ALIVE, primary_node, {check_alive}),
			% From the neighbours list we remove the ones that have not contacted the primary recently
      Alive_Neigh_list = check_alive(Neigh_list),
			% Neigh_list is a list of couple <(?),network_join_instant>, since we have to send an heartbeat message to all the
			% secondary server we take only the list of (?) via list comprehensions
			Alive_Node_list=[X||{X,_} <- Alive_Neigh_list],
			% Print all the elements of the Alive_Neigh_list
	  	lists:foreach(fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list),
	  	Dead_Neigh_list= get_diff(Neigh_list,Alive_Node_list),
	  	io:format("---------SECONDARY NODES--------~n"),
			case Dead_Neigh_list == [] of
				false ->
					% if at least one node fails primary propagates to all the secondary the neighbour_del_propagation in order to
					% make them aware from the fact that one of its neighbour has failed.
					lists:foreach( fun(H) -> io:format("~p HAS BEEN REMOVED FROM THE CLUSTER...~n", [H]) end, Dead_Neigh_list),  %%DEBUG
					lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list),
					io:format("~n"),
					Dead_Node_list=[X||{X,_} <- Dead_Neigh_list],
					% The message containing all the nodes that have failed is sent to the secondary nodes. The broadcast call
					% returns the list of nodes that instead does not respond with an acknowledgment to the
					% neighbour_del_propagation message.
					Failed_Node_List = broadcast_call(Alive_Node_list,{neighbour_del_propagation,Dead_Node_list}),

					if
						Failed_Node_List == [] ->
							% No node has failed during the propagation of the neighbour_del_propagation message
							io:format("[primary_node] Nodes have been correctly deleted.~n");

						true ->
							io:format("[primary_node] Following nodes have failed during delete propagation and will be removed: ~w ~n", [Failed_Node_List])
					end,

					% Finally the state of the current server - the list of neighbour nodes alive -  is updated
					New_Neigh_list = get_diff(Alive_Neigh_list,Failed_Node_List),
					{noreply, lists:reverse(lists:keysort(1,New_Neigh_list))};
				true ->
					case Alive_Neigh_list == [] of
						false ->
							lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list);  %%DEBUG
						true ->
							io:format("There are no secondary nodes...~n~n")
					end,
					io:format("~n"),
					{noreply,lists:reverse(lists:keysort(1, Alive_Neigh_list))}
			end;

		_Dummy ->
      io:format("[primary node] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy]), % DEBUG
      {noreply, Neigh_list}
  end.

terminate(_Reason, _State) ->
  ok.
  
check_alive(Neigh_list) ->
  Now = erlang:monotonic_time(millisecond),
	% Remove from the neighbours list the ones that have not contacted the primary server in the last 2 * TIMEOUT_ALIVE
	% milliseconds, exploiting the list comprehension.
  [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Neigh_list, Now - Last_time_contact < 2 * ?TIMEOUT_ALIVE].
  							  
								  
broadcast_call(_, {}) ->
  empty_message;
  

broadcast_call(L, Msg) ->
	io:format("CHIAMO broadcast_call(L, Msg)"), % DEBUG
	% broadcast_call(L, Msg, []) returns -if there are- the list of nodes that have failed during the secondary_node
	% message broadcasting.
	Nodes_to_delete = broadcast_call(L, Msg, []),
	io:format("@@@@NODE_TO_DELETE  ~w  ~n",[Nodes_to_delete]), % DEBUG
	if
		Nodes_to_delete == [] ->
			% If no nodes have failed during the propagation of the message broadcast_call ends
			[];
		  
		true ->
			% Obtain the new list of alive nodes subtracting the ones that have failed
			Alive_nodes = get_diff(L, [Nodes_to_delete]),
			% Propagate the neighbour_del_propagation in order to inform the alive neighbours that some nodes have just failed
			X = [Nodes_to_delete] ++ broadcast_call(Alive_nodes, {neighbour_del_propagation, Nodes_to_delete}),
			io:format("@@@@NODES_TO_DELETE   ~w  ~n",[X]), % DEBUG
			% The list of all the new failed nodes is returned
			X
	end.
  
broadcast_call([], Msg, L) ->
	io:format("CHIAMO broadcast_call([], Msg, L)"), % DEBUG
	L;
	
broadcast_call([H|T], Msg, L) ->
	% Via gen_server:call the server makes a synchronous call to the server with Name = secondary_node and located at the
	% node contained inside the list passed as argument(list of alive nodes). In particular it sends a request and waits
	% until a reply arrives or a time-out occurs.
	% catch(?)
	Reply =(catch gen_server:call({secondary_node, H}, Msg, ?CALL_TIMEOUT)),
	io:format("Call reply: Reply = ~w  ~n",[Reply]), % DEBUG
	case Reply of 

		update_neighbours_reply ->
			broadcast_call(T, Msg, L);

		{add_point_reply, Result} ->
			case Result of
		
				{updated,1} -> 
					io:format("[primary node] RM ~w has correctly added the point ...~n~n", [H]), % DEBUG
					broadcast_call(T, Msg, L);
				_ ->
					io:format("[primary node] An error occured with RM ~w during the point addition  ...~n~n", [H]), % DEBUG
					% Remove H from the list of alive nodes (?)
					broadcast_call(T, Msg, L)
			end;
			
		{check_db_consistency_reply, Result} ->
			case Result of
		
				{updated,1} -> 
					io:format("[primary node] RM ~w has correctly added the point ...~n~n", [H]), % DEBUG
					broadcast_call(T, Msg, L);
					
				{selected, _, _} ->
				
					io:format("[primary node] RM ~w 's DB is already consistent ...~n~n", [H]), % DEBUG
					broadcast_call(T, Msg, L);
					
				_ ->
					io:format("[primary node] An error occured with RM ~w during the point addition  ...~n~n", [H]), % DEBUG
					% Remove H from the list of alive nodes (?)
					broadcast_call(T, Msg, L)
			end;
			
		_ ->
			io:format("[primary node] An error has occured. Node ~w must be removed from the list of secondary nodes. ~n", [H]),
			% Stores the current node, that has failed, in order to return it to at the end of the broadcast call
			broadcast_call(T, Msg, L ++ H)
		end.
	
	
broadcast_cast(_, {}) ->
	empty_message;
  
broadcast_cast([], Msg) ->
	ok;
	
broadcast_cast([H|T], Msg) ->
	% gen_server:cast sends an asynchronous request to the server whose name is secondary_node and is located at node H
	gen_server:cast({secondary_node, H}, Msg),
	% In an iterative manner the message is sent to all the nodes contained in the list elements
	broadcast_cast(T, Msg).
  
update_last_contact(RM_id_target,Neighbours_list) ->
	% This function updates the information about the last contact with the node passed as argument
  Now = erlang:monotonic_time(millisecond),
  New_Neighbours_list = [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Neighbours_list, RM_id_target /= RM_id],
  New_Neighbours_list ++ [{RM_id_target, Now}]. 


get_diff([],L2) ->
	[];
	
get_diff([H|T],L2) ->
	if 
		is_tuple(H) ->
			% If the current element under analysis is a tuple only the first element is taken: the server name
			Name = element(1,H);
		true ->
			Name = H
	end,
	io:format("  L2 :  ~w~n", [L2]),
	io:format("  H , NAME :  ~p , ~p~n", [H, Name]),
	% lists:member returns true if the the element Name belongs to the list L2
	case lists:member(Name, L2) of
		true ->
			get_diff(T,L2);
		false ->
			% If the element does not belong to the second list its added to the list of different elements
			[H] ++ get_diff(T,L2)
	end.
	
% Function that signal via TCP socket that a new primary was elected
update_primary() ->
	SomeHostInNet = "localhost",
	{ok,Socket} = gen_tcp:connect(SomeHostInNet,?PORT,[binary,{packet,0}]),
	ok = gen_tcp:send(Socket, pid_to_list(self()) ++ ";" ++ atom_to_list(node())),
	ok = gen_tcp:close(Socket).
 
get_node_id() ->
	Y = string:substr(atom_to_list(node()),5,2).

stop() ->
	exit(whereis(?SERVER), ok).

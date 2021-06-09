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

-export([start_link/1, init/1, elect/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, stop/0]).


-include("utility.hrl").

-define(SERVER, ?MODULE).

-record(rm_state,{neighbours_list}).


start_link(Neighbours_list) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Neighbours_list, []).

init(Neighbours_list) ->
  
  io:format("Primary node has been created ~n~n"),
  erlang:send_after(?TIMEOUT_ALIVE, primary_node, {check_alives}), %%handled by handle_info callback
  erlang:send_after(?TIMEOUT_ALIVE, primary_node, {send_heartbeat}), %%handled by handle_info callback

  {ok,Neighbours_list}. 
  
elect(Neighbours_list) ->

	gen_server:call({secondary_node, node()}, stop, ?CALL_TIMEOUT),
	io:format("Secondary node has been terminated ~n~n"),

	start_link(Neighbours_list).
  
handle_cast(Request, Neigh_list) -> %Asynchronous request
 %% io:format("Cast requested: Request = ~w ~n",[Request]), % DEBUG
  % format_state(State), % DEBUG
  case Request of
  
    {heartbeat,Neigh} when is_atom(Neigh) ->
  %%    io:format("[primary node] received a heartbeat mex from RM: ~w~n", [Neigh]), % DEBUG
	  New_Neigh_list=update_last_contact(Neigh,Neigh_list),
      {
        noreply,
        New_Neigh_list
      };

    %% catch all clause
    _ ->
      io:format("[primary_node] WARNING: bad request format~n"),
      {noreply, Neigh_list}

  end.



handle_call(Request, From, Neigh_list) -> %Synchronous request
  io:format("Call requested: Request = ~w ~n",[Request]), % DEBUG
  %format_state(State), % DEBUG
  case Request of
	%%UN NODO MANDA UNA RICHIESTA DI ADD AL PRIMARIO CHE LA INOLTRA AI SECONDARI
    {neighbour_add, New_Neigh} when is_atom(New_Neigh) ->
	  Now = erlang:monotonic_time(millisecond),
	  if 
		Neigh_list == [] ->
		
		  New_Neigh_list = [{New_Neigh,Now}],
		  Returned_Node_list=[X||{X,_} <- Neigh_list],
		  io:format("[primary_node] Node ~w has been correctly added.~n", [New_Neigh]);
		  
		true ->
		
		  Node_list=[X||{X,_} <- Neigh_list], %%We only consider the first element in each tuple...
		  Failed_Node_List = broadcast_call(Node_list,{neighbour_add_propagation,New_Neigh}),
		  
		  if 
			Failed_Node_List == [] ->
				io:format("[primary_node] Node ~w has been correctly added.~n", [New_Neigh]),
				New_Neigh_list = Neigh_list ++ [{New_Neigh,Now}],
				Returned_Node_list = Node_list;
			true ->
			
				io:format("[primary_node] Following nodes have failed and will be removed: ~w ~n", [Failed_Node_List]),
				
				New_Neigh_list= get_diff(Neigh_list ++ [{New_Neigh,Now}],Failed_Node_List),
				Returned_Node_list = [X||{X,_} <- get_diff(New_Neigh_list, [New_Neigh])],
				io:format("[primary_node] New neighbors: ~w ~n", [New_Neigh_list])

				
			end
		  
		end,
      {
        reply,   
		Returned_Node_list,			%%Il nuovo nodo riceve la lista dei nodi, escluso sè stesso
        New_Neigh_list	
      };

  
	{get_map, Lat, Lng, Radius} ->
		Point="POINT(" ++ Lat ++ " " ++ Lng ++ ")",
		io:format("[primary_node] received a get_map request. Point: ~p, Radius: ~p ~n", [Point, Radius]),

	    Query="SELECT * FROM points WHERE ST_Distance(geog, ST_GeographyFromText('" ++ Point ++ "')) < " ++ Radius ++ ";",
	    odbc:start(),
	    {ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgres;UID=postgres;PWD=admin",[]),
        Result=odbc:sql_query(Ref, Query),
		io:format("~p~n", [Result]),
		io:format("[primary_node] GET MAP operation correctly performed.~n"),
		%%TODO METTERE CASE OF RESULT E RESTITUIRE SUCCESS/ERROR
		{reply, Result, Neigh_list};
	
	{add_point, Type , Lat, Lng, Max_speed} ->
		Point="POINT(" ++ Lat ++ "," ++ Lng ++ ")",
		io:format("[primary_node] received an add_point request. Type: ~p, Point: ~p, Max speed: ~p~n", [Type, Point, Max_speed]),
		if 
		Neigh_list =/= [] ->
		
		  Node_list=[X||{X,_} <- Neigh_list], %%We only consider the first element in each tuple...
		  
			Failed_Node_List = broadcast_call(Node_list,{add_point, Type , Lat, Lng, Max_speed}),
			if 
				Failed_Node_List == [] ->
					io:format("[primary_node] ADD POINT operation correctly performed.~n"),
					New_Neigh_list = Neigh_list;
				
				true ->
					io:format("[primary_node] Following nodes have failed and will be removed: ~w ~n", [Failed_Node_List]),
					New_Neigh_list= get_diff(Neigh_list,Failed_Node_List)
			end,
			  		  	  
		   Query="INSERT INTO points(type,point,max_speed) VALUES ('" ++ Type ++ "', '" ++ Point ++ "', '" ++ Max_speed ++ "');",
		   odbc:start(),
		   {ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgres;UID=postgres;PWD=admin",[]),
		   Result=odbc:sql_query(Ref, Query),
		   case Result of 
				{updated,1} -> 
					io:format("[primary node] Primary node has correctly added the point ...~n~n"), % DEBUG
					{reply, success, New_Neigh_list};
				_ ->
					io:format("[primary node] An error occured with primary node during the point addition  ...~n~n"), % DEBUG
					{reply, error, New_Neigh_list}
		    end;


		true ->
		   Query="INSERT INTO points(type,point,max_speed) VALUES ('" ++ Type ++ "', '" ++ Point ++ "', '" ++ Max_speed ++ "');",
		   odbc:start(),
		   {ok, Ref} = odbc:connect("Driver={PostgreSQL ODBC Driver(UNICODE)};Server=127.0.0.1;Port=5432;Database=postgres;UID=postgres;PWD=zaccaria97",[]),
		   Result=odbc:sql_query(Ref, Query),
		   case Result of 
				{updated,1} -> 
					io:format("[primary node] Primary node has correctly added the point ...~n~n"), % DEBUG
					{reply, success, Neigh_list};
				_ ->
					io:format("[primary node] An error occured with primary node during the point addition  ...~n~n"), % DEBUG
					{reply, error, Neigh_list}
		    end
		end;
  
    %% catch all clause
    _ ->
      io:format("[primary node] WARNING: bad request format~n"),
      {reply, bad_request, Neigh_list}
  end.
  
  
  
%%callback from send_after, or in general all ! messages
handle_info(Info, Neigh_list) ->
  case Info of
	{send_heartbeat} ->
	  io:format("------------------------~n"),
      io:format("[primary node] heartbeat is sent to secondary nodes...~n~n"), % DEBUG
	  io:format("-------------------~n"),

	  Node_list=[X||{X,_} <- Neigh_list], %%We only consider the first element in each tuple...
	  broadcast_cast(Node_list,{heartbeat,node()}),
      erlang:send_after(?TIMEOUT_ALIVE, primary_node, {send_heartbeat}),
      {
        noreply,
        Neigh_list
      };
  
    {check_alives} ->
  %%    io:format("Timeout expired: time to check rms alives ~n"), % DEBUG
      erlang:send_after(?TIMEOUT_ALIVE, primary_node, {check_alives}),
      Alive_Neigh_list = check_alives(Neigh_list),
	  Alive_Node_list=[X||{X,_} <- Alive_Neigh_list], %%We only consider the first element in each tuple...
	  lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list),
	  Dead_Neigh_list= get_diff(Neigh_list,Alive_Node_list),
	  io:format("---------SECONDARY NODES--------~n"),
		
	  case Dead_Neigh_list == [] of
		false -> 
			
			lists:foreach( fun(H) -> io:format("~p HAS BEEN REMOVED FROM THE CLUSTER...~n", [H]) end, Dead_Neigh_list),  %%DEBUG
			lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list),
			io:format("~n"),
			Dead_Node_list=[X||{X,_} <- Dead_Neigh_list], 
			Failed_Node_List = broadcast_call(Alive_Node_list,{neighbour_del_propagation,Dead_Node_list}),
			
			if 
			Failed_Node_List == [] ->
			
				io:format("[primary_node] Nodes have been correctly deleted.~n");
				
				
			true ->
			
				io:format("[primary_node] Following nodes have failed during delete propagation and will be removed: ~w ~n", [Failed_Node_List])
				
			end,
			
			New_Neigh_list = get_diff(Alive_Neigh_list,Failed_Node_List),
			{
				noreply,
				New_Neigh_list
			};
		true ->
			case Alive_Neigh_list == [] of
				false -> 
					lists:foreach( fun(H) -> io:format("~p~n", [H]) end, Alive_Neigh_list);  %%DEBUG
				true ->
					io:format("There are no secondary nodes...~n~n")
				end,
			io:format("~n"),
			{
				noreply,
				Alive_Neigh_list
			}
		end;
		
    _Dummy ->
      io:format("[dispatcher] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy]), % DEBUG
      {noreply, Neigh_list}
  end.

terminate(_Reason, _State) ->
  ok.
  
  
  


check_alives(Neigh_list) ->
  Now = erlang:monotonic_time(millisecond),
  [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Neigh_list,
                                  Now - Last_time_contact < 2 * ?TIMEOUT_ALIVE].
  							  
								  
broadcast_call(_, {}) ->
  empty_message;
  

broadcast_call(L, Msg) ->
	io:format("CHIAMO broadcast_call(L, Msg)"), % DEBUG

	Nodes_to_delete = broadcast_call(L, Msg, []),
	io:format("@@@@NODE_TO_DELETE  ~w  ~n",[Nodes_to_delete]), % DEBUG
	if
		Nodes_to_delete == [] ->
		
			[];
		  
		true ->
			New_list = get_diff(L, [Nodes_to_delete]),
			X = [Nodes_to_delete] ++ broadcast_call(New_list, {neighbour_del_propagation, Nodes_to_delete}),
			io:format("@@@@NODES_TO_DELETE   ~w  ~n",[X]), % DEBUG
			X
		  
	  end.
  
broadcast_call([], Msg, L) ->
	io:format("CHIAMO broadcast_call([], Msg, L)"), % DEBUG

	L;
	
broadcast_call([H|T], Msg, L) ->
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
					broadcast_call(T, Msg, L)
			end;
			
		_ ->
			io:format("[primary node] An error has occured. Node ~w must be removed from the list of secondary nodes. ~n", [H]),
		%%	Dead_Node_list=[X||{X,_} <- Dead_Neigh_list], 
		%%	broadcast_call(Node_list,{neighbour_del_propagation,Dead_Node_list}),
		%%	brodcast_call(, ),
			broadcast_call(T, Msg, L ++ H)
		end.
	
	
broadcast_cast(_, {}) ->
    empty_message;
  
broadcast_cast([], Msg) ->
	ok;
	
broadcast_cast([H|T], Msg) ->
    gen_server:cast({secondary_node, H}, Msg),
	broadcast_cast(T, Msg).
		
  
  
update_last_contact(RM_id_target,Neighbours_list) ->
  Now = erlang:monotonic_time(millisecond),
  New_Neighbours_list = [{RM_id, Last_time_contact} || {RM_id, Last_time_contact} <- Neighbours_list, RM_id_target /= RM_id],
  New_Neighbours_list ++ [{RM_id_target, Now}]. 


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
	

 
stop() ->
	exit(whereis(?SERVER), ok).

% THE ALGORITHM (adapted from https://en.wikipedia.org/wiki/Bully_algorithm)
%
% Three types of message:
% 1. Election Message: Sent to announce election;
% 2. Answer Message: Responds to the Election message;
% 3. Leader Message: Sent by winner of the election to announce victory.
% When the current coordinator has failed, P performs the following actions:
%
% (1) If P has the highest process ID, it sends a Leader message to all other processes and becomes
% 		the new Coordinator. Otherwise, P broadcasts an Election message to all other processes with
% 		higher process IDs than itself.
%
% (2) If P receives no Answer after sending an Election message, then it broadcasts a Leader message
% 		to all other processes and becomes the Coordinator.
%
% (3) If P receives an Answer from a process with a higher ID, it waits for a Leader message (if there
% 		is no Leader message after a period of time, it restarts the process at the beginning).
%
% (4) If P receives an Election message from another process with a lower ID it sends an Answer message
% 		back and starts the election process at the beginning, by sending an Election message to
% 		higher-numbered processes.
%
% (5) If P receives a Victory message, it treats the sender as the coordinator.
%
% IMPLEMENTATION DETAILS:
% The state of the gen_server is represented by
% - the list of neighbours
% - the newly elected primary node when it is determined
%
% (?) La lista dei neighbours vale ancora
% (?) I secondary iniziano contemporaneamente il bully algorithm
% (?) Manca il modo di far ripartire il server secondario con l'informazione sul nuovo nodo

-module(bully_algorithm).

-behaviour(gen_server).

-define(SERVER,?MODULE).
-include("utility.hrl").

-export([start_link/1, start_election_process/0, start_election_process/1, init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).


start_link(Neighbours) ->
	% Start link arguments:
	% - {local, ?SERVER} -> register the server locally associating the name via MACRO
	% - The second argument is the name of the callback module where the callback functions are located, in this case is
	%   is the current module
	% - Neighbours_list is passed as argument to the callback function init
	gen_server:start_link({local, ?SERVER}, ?MODULE, Neighbours, []).

init(Neighbours) ->
	% If name registration succeeds this function will be called
	io:format("~n ...Starting the bully algorithm... ~n"),
	% The function is expected to return {ok,State} where State is the internal state of the gen_server
	{ok,{Neighbours,no_leader}}.

start_election_process() ->
	% Stops the process that manages the normal behaviour of the secondary node
	gen_server:call({secondary_node, node()}, stop, ?CALL_TIMEOUT),
	spawn(primary_node, elect, []).
	
start_election_process(Neighbours) ->
	% Stops the process that manages the normal behaviour of the secondary node
	gen_server:call({secondary_node, node()}, stop, ?CALL_TIMEOUT),
	% Start the bully algorithm
	Result = start_link(Neighbours),
	Result_atom = element(1,Result),
	if
		Result_atom == ok ->
			start_election(Neighbours);
		true ->
			io:format("~n ...An error occurred, node will shut down... ~n")
			% (?) Come si termina il processo corrente
	end.

	
start_election(Neighbours_list) ->

	Higher_id_list = get_neighbours_with_higher_id(Neighbours_list),
	io:format("@@@@@Higher_id_list ~w~n",[Higher_id_list]), % DEBUG
	if
		Higher_id_list == [] ->
			% (1)  Case the current node is the winner: the node must be sure that the other nodes have detected the primary node failure and have started the election mechanism, otherwise the broadcast_call will have no effect.
			erlang:send_after(?VICTORY_TIMEOUT, bully_algorithm, {notify_victory});

		true ->
			erlang:send_after(?VICTORY_TIMEOUT, bully_algorithm, {check_leader})
	end.

handle_info(Info,{Neighbours,Leader}) ->
	case Info of
		{check_leader} ->
			if
				Leader == no_leader ->
					% (3) No victory message received after VICTORY_TIMEOUT milliseconds the election process must be restarted
					start_election(Neighbours);
				true ->
					spawn(secondary_node, restart_link, [Leader])
			end;
			
		{notify_victory} ->
		
			broadcast_leader(Neighbours),
			io:format("@@@@@BROADCAST HAS TERMINATED~n"), % DEBUG
			spawn(primary_node, elect, []); 
			%%spawn(primary_node, elect, [Neighbours]), 
			
		_Dummy ->
			io:format("[bully_algorithm] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end,
	{noreply,{Neighbours,Leader}}.


handle_call(stop, From, {Neighbours,Leader}) ->
    {stop, normal, shutdown_ok, {Neighbours,Leader}};
	
	
handle_call(Request,From,{Neighbours,Leader}) ->

	io:format("[bully_algorithm] CALL Request: ~w ~n",[Request]), %%DEBUG

	case Request of
		{Node,election} ->
			if
				Node < node()->
					%(4) Case I receive an election message from a process with lower ID w.r.t mine I have to restart the election mechanism;
					erlang:send_after(?VICTORY_TIMEOUT, secondary_node, {check_leader})
			end,
			{reply,answer,{Neighbours,Leader}};
			
		_Dummy ->
			io:format("[bully_algorithm] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end.

handle_cast(Request, {Neighbours,Leader}) ->

	io:format("[bully_algorithm] CAST Request: ~w ~n",[Request]), %%DEBUG
	case Request of
		{Node,leader} ->
			% (5) If the node receive a Victory message it assumes that the sending node is the leader
			%%spawn(secondary_node, restart_link, [Node]),
			{noreply, {Neighbours, Node}};
			
		{Node,leader} ->
			% (5) If the node receive a Victory message it assumes that the sending node is the leader
			%%spawn(secondary_node, restart_link, [Node]),
			{noreply, {Neighbours, Node}};
		
		_Dummy ->
			io:format("[bully_algorithm] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end.

get_neighbours_with_higher_id([]) ->
	[];

get_neighbours_with_higher_id(L) ->
	
	if 
		is_list(L)-> 
			Neighbours = L;
		true ->
			Neighbours = [L]
		end,

	Nodes_With_Higher_Id = [Neighbour || Neighbour <- Neighbours, Neighbour > node()],
	io:format("@@@@@Nodes_With_Higher_Id ~w~n",[Nodes_With_Higher_Id]), % DEBUG
	Nodes_With_Higher_Id.

broadcast_leader([]) ->
	ok;

broadcast_leader([H|T]) ->
	% If I'm the election winner I don't have to wait any reply from the nodes
	
	io:format("[bully_algorithm] broadcast_leader on node: ~w ~n",[H]),
		%%Reply =(catch gen_server:call({bully_algorithm,H},{node(),leader}, ?CALL_TIMEOUT)),
	gen_server:cast({bully_algorithm,H},{node(),leader}),
	broadcast_leader(T);
	
broadcast_leader(H) ->
	% If I'm the election winner I don't have to wait any reply from the nodes
	
	io:format("[bully_algorithm] broadcast_leader on node: ~w ~n",[H]),
	Reply =(catch gen_server:call({bully_algorithm,H},{node(),leader}, ?CALL_TIMEOUT)),
	broadcast_leader([]).

broadcast_election(Neighbours) ->
	% multi_call(Nodes, Name, Request) makes a synchronous call to all gen_server processes locally
	% registered as Name at the specified Nodes by first sending the request to every node and then
	% waits for the replies. Where Replies is a list of {Node,Reply} and BadNodes is a list of node
	% that either did not exist, or where the gen_server Name did not exist or did not reply.
	{Replies,_} = gen_server:multi_call(Neighbours,bully_algorithm,{node(),election},?ELECTION_TIMEOUT),
	% Gets the all the list of nodes that have replied with an answer message to the election
	Multi_call_tuple = [Tuple || Tuple <- Replies, element(2,Tuple) == answer],
	Alive_nodes = [Node || {_,Node} <- Multi_call_tuple],
	if
		Alive_nodes == [] ->
			no_answer;
		true ->
			lists:max(Alive_nodes)
	end.

terminate(_Reason, _State) ->
	ok.

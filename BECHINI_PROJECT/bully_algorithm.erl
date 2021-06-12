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
% (3) If P receives an Answer from a process with a higher ID, it waits for a Victory message (if there
% 		is no Victory message after a period of time, it restarts the process at the beginning).
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

-export([start_link/1, start_election_process/1, init/1, terminate/2, handle_info/2, handle_call/3, handle_cast/2]).


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
			io:format("~n ...An error occurred, node will shut down... ~n"),
			% (?) Come si termina il processo corrente
	end.

start_election(Neighbours_list) ->
	Higher_id_list = get_neighbours_with_higher_id(Neighbours_list),
	if
		Higher_id_list == [] ->
			% (1)  Case the current node is the winner
			broadcast_leader(Neighbours_list),
			spawn(primary_node, elect, []);
		true ->
			% (1) Case there are node with id greater than mine
			Leader_Node = broadcast_election(Higher_id_list),
			if
				Leader_Node == no_answer ->
				 % (2) Case no nodes reply with an answer to the election message
				 broadcast_leader(Neighbours_list);
				true ->
					% (3) Node must wait for a victory message
					erlang:send_after(?VICTORY_TIMEOUT, secondary_node, {check_leader})
			end
	end.

handle_info(Info,{Neighbours,Leader}) ->
	case Info of
		{check_leader} ->
			if
				Leader == no_leader ->
					% (3) No victory message received after VICTORY_TIMEOUT milliseconds the election process must be restarted
					start_election(Neighbours)
			end;

		_Dummy ->
			io:format("[dispatcher] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end,
	{noreply,{Neighbours,Leader}}.


handle_call(Request,From,{Neighbours,Leader}) ->
	case Request of
		{Node,election} ->
			if
				Node < node()->
					%(4) Case I receive an election message from a process with lower ID w.r.t mine I have to restart the election mechanism;
					erlang:send_after(?VICTORY_TIMEOUT, secondary_node, {check_leader})
			end,
			{reply,answer,{Neighbours,Leader}};
		_Dummy ->
			io:format("[dispatcher] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end.

handle_cast(Request, {Neighbours,Leader}) ->
	case Request of
		{Node,leader} ->
			% (5) If the node receive a Vicory message it assumes that the sending node is the leader
			{noreply, {Neighbours, Node}};
		_Dummy ->
			io:format("[dispatcher] WARNING: bad mex format in handle_info Format ~w ~n",[_Dummy])
	end.

get_neighbours_with_higher_id(Neighbours) ->
	[Neighbour || Neighbour <- Neighbours, Neighbour > node()].

broadcast_leader([]) ->
	ok;

broadcast_leader([Current|Neighbours]) ->
	% If I'm the election winner I don't have to wait any reply from the nodes
	gen_server:cast({secondary_node,Current},{node(),leader}),
	broadcast_leader(Neighbours).

broadcast_election(Neighbours) ->
	% multi_call(Nodes, Name, Request) makes a synchronous call to all gen_server processes locally
	% registered as Name at the specified Nodes by first sending the request to every node and then
	% waits for the replies. Where Replies is a list of {Node,Reply} and BadNodes is a list of node
	% that either did not exist, or where the gen_server Name did not exist or did not reply.
	{Replies,_} = gen_server:multi_call(Neighbours,secondary_node,{node(),election},?ELECTION_TIMEOUT),
	% Gets the all the list of nodes that have replied with an answer message to the election
	Multi_call_tuple = [Tuple || Tuple <- Replies, elem(2,Tuple) == answer],
	Alive_nodes = [Node || {_,Node} <- Multi_call_tuple],
	if
		Alive_nodes == [] ->
			no_answer;
		true ->
			lists:max(Alive_nodes)
	end.

terminate(_Reason, _State) ->
	ok.
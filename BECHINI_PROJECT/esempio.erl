-module(esempio).
  
-export([init/0]).

  
init() ->  
	X=[string:substr(atom_to_list(Node_name),5,2) || {Node_name, _} <- [{node(),null}]],
	X.

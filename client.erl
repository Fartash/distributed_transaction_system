%% - Client module
%% - The client module creates a parallel process by spawning handler. 
%% - The handler does the following: 
%%      1/ It makes itself into a system process in order to trap exits.
%%      2/ It creates a window and sets up the prompt and the title.
%%      4/ It waits for connection message (see disconnected).
%%

-module(client).

-import(window, [set_title/2, insert_str/2, set_prompt/2]).

-export([start/1, return_nth_element/2, order_aux/3, return_elements/3, count/2]).

start(Host) ->
    spawn(fun() -> handler(Host) end).

%%%%%%%%%%%%%%%%%%%%%%% INACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The handler starts a window and a connector
handler(Host) ->
    process_flag(trap_exit, true),
    Window = window:start(self()),
    set_title(Window, "Connecting..."),
    set_prompt(Window, "action > "),
    start_connector(Host),
    disconnected(Window).

%% - The window is disconnected until it received a connected meassage from 
%% the connector
disconnected(Window) ->
    receive
	{connected, ServerPid} -> 
	    insert_str(Window, "Connected to the transaction server\n"),
	    set_title(Window, "Connected"),
	    connected(Window, ServerPid);
	{'Exit', _, _} -> exit(died);
	Other -> io:format("client disconnected unexpected:~p~n",[Other]),
		 disconnected(Window)
    end.
%%%%%%%%%%%%%%%%%%%%%%% INACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% CONNECTOR %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_connector(Host) ->
    S = self(),
    spawn_link(fun() -> try_to_connect(S,Host) end).

try_to_connect(Parent, Host) ->
    %% Parent is the Pid of the process (handler) that spawned this process
    io:format("try_to_connect~n"),
    {transaction_server, Host} ! {login, self(), Parent},
    receive 
	{ok, ServerPid} -> Parent ! {connected, ServerPid},
			   d('received an ok from server'),
			   exit(connectorFinished);
	Any -> io:format("Unexpected message~p.~n",[Any])
    after 10000 ->
	io:format("Unable to connect to the transaction server at node~p. Restart the client application later.~n",[Host])
    end,
    exit(serverBusy).
%%%%%%%%%%%%%%%%%%%%%%% CONNECTOR %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




%%%%%%%%%%%%%%%%%%%%%%% ACTIVE CLIENT %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connected(Window, ServerPid) ->
    receive
	%% - The user has requested a transaction
	{request, Window, Transaction} ->
	    io:format("Client requested the transaction ~p.~n",[Transaction]),
	    insert_str(Window, "Processing request...\n"),
	    process(Window, ServerPid, Transaction);
	{'EXIT', Window, windowDestroyed} -> end_client(ServerPid);
	{close, ServerPid} -> exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other]),
	    connected(Window,ServerPid)
    end.

%% - Asking to process a request
process(Window, ServerPid, Transaction) ->
    Transaction_with_seq = add_seq(Transaction, [], 1),
    Transaction_with_No = add_count(Transaction_with_seq),
    io:format("Transaction along with sum check: ~p.~n",[Transaction_with_No]),
    ServerPid ! {request, self()}, %% Send a request to server and wait for proceed message
    d('sent a transaction request message to server'),
    receive
	{proceed, ServerPid} -> send(Window, ServerPid, Transaction_with_No), %% received green light send the transaction.
	d('received a transaction request green light from server');
	{close, ServerPid} -> exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    end.

%% - Sending the transaction and waiting for confirmation
send(Window, ServerPid, Transaction) ->
    send(Window, ServerPid, Transaction, Transaction).
send(Window, ServerPid, [], Transaction) ->
    ServerPid ! {confirm, self()}, %% Once all the list (transaction) items sent, send confirmation
    d('sent last chunk of transaction. sent confirm message'),
    receive
	{abort, ServerPid} -> insert_str(Window, "Aborted... type run if you want to try again!\n"),
		       connected(Window, ServerPid);
	{committed, ServerPid} -> insert_str(Window, "Transaction succeeded!\n"),
			  connected(Window, ServerPid);

	{count_missing, ServerPid} -> ServerPid ! {count, count(Transaction, -1)},
					io:format("Server said count is missing. sent count: ~p~n", [count(Transaction, -1)]),
					sleep(8),
					send(Window, ServerPid, [], Transaction);

        {chunks_missing, ServerPid, Chunk_no} ->
    			Missed_elements = return_elements(Chunk_no, Transaction),
			io:format("missed element: ~p~n", [Missed_elements]),
    			ServerPid ! {missed_chunk_resent, Missed_elements},
    			io:format("resent chunks: ~p~n", [Missed_elements]),
			insert_str(Window, "resent missing chunk(s) of transaction!\n"),
    			sleep(8),
			
			send(Window, ServerPid, [], Transaction);

	{complete_resend_request, ServerPid} ->
			io:format("resending the whole transaction again~n"),
			ServerPid ! {complete_transaction_again, Transaction},  
			insert_str(Window, "resent the whole transaction again!\n"), 
			sleep(8),
			send(Window, ServerPid, [], Transaction);

	{'EXIT', Window, windowDestroyed} -> end_client(ServerPid);
	{close, ServerPid} -> 
	    exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    end;
send(Window, ServerPid, [H|T], Transaction) -> 
    d('sent a chunk of transaction'),
    sleep(1), 
    case loose(5) of
	false -> ServerPid ! {action, self(), H}; 
        true -> ok
    end,
    send(Window, ServerPid, T, Transaction).
%%%%%%%%%%%%%%%%%%%%%%% Active Window %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%% - Clean end
end_client(ServerPid) ->
    io:format("Client ended communication.~n",[]),
    ServerPid ! {close, self()},
    exit(died).

%% - Blocks a random amount of seconds between 1 and 5.
%% - This simulates latency in the network.
%% - Latency is an integer parameter which can be interpreted as a worst case 
%% waiting time in seconds
sleep(Latency) ->
    receive
    after 10*random:uniform(Latency) ->
	  true
    end.

%% - Loses messages randomly
%% - This simulates the fact that the communication media is unreliable
%% - Lossyness is an integer parameter:
%%        - if Lossyness =0 the function will always return true
%%        - if Lossyness >10 the function will always return false
%%        - if lossyness =6 the function will return either values with 
%%        probability 1/2    
loose(Lossyness) -> 
    Val=random:uniform(10),
    if  
	Val >= Lossyness -> false;
	true -> true
    end.
    
%%-------------Count--------------------
%% counts the number of elements in a transaction.
count([], No) ->
    No;
count([_H|T], No) ->
    count(T, No+1).

%%--------------------------------DEBUG--------------------

d(_Info) ->
    %%io:format("client module: ~w~n", [Info]),
    ok.


%%-----------------------------------------------------------
add_count(Transaction) ->
    No = count(Transaction, 0),
    [No|Transaction].

%%-----------------------------------------------------------

add_seq([], New_transaction, _No) ->
    New_transaction;
add_seq([H|T], New_transaction, No) ->
    add_seq(T, [[No, H] | New_transaction], No+1).
	    
%%------------------------------------------------------------
return_nth_element([[No, Data]|T], No) ->
    [No, Data];
return_nth_element([Other|T], No) ->
    return_nth_element(T, No).

%%-----------------------------------------------------------
return_elements(Elements, [H|Transaction]) ->
    return_elements(Elements, Transaction, []).
return_elements([], _Transaction, Result) ->
    Result;
return_elements([H|T], Transaction, Result) ->	
    return_elements(T, Transaction, [return_nth_element(Transaction, H)|Result]).

%%-----------------------------------------------------------


%%-----------------------------------------------------------
reverse(List) ->
    reverse(List, []).

reverse([], Buffer) ->
    Buffer;
reverse([H|T], Buffer) ->
    reverse(T, [H|Buffer]).

%%---------------------------------------------------------
 

%%---------------------------------------------------------
order_aux(_Element, [], Result) ->
    reverse(Result);
order_aux(Element, [H|T], Result) ->
    case Element > H of
	true ->
		order_aux(Element, T, [H|Result]);
	false ->
		New_result = [Element] ++ Result,
		order_aux([], T, [H|New_result])
    end.


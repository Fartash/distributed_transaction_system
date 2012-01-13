%% This module has been written merely for test purposes. It
%% creates and sends a large number of requests to the server
%% to study its behavior under stress.


-module(clients).

-export([start/2]).


start(_Host, 0) ->
    io:format("all processes created~n");
start(Host, No) ->
    spawn_N(Host, No),
    start(Host, No-1).
spawn_N(Host, No) ->
    Pid = self(),
    spawn(fun() -> login(Pid, Host, No) end).

login(Parent, Host, No) ->
    sleep(random:uniform(No)),
    io:format("try_to_connect ~p~n", [No]),
    {transaction_server, Host} ! {login, self(), self()},
    receive 
	{ok, ServerPid} -> process(a, ServerPid, [{read,a}]),
			   exit(connectorFinished);
	Any -> io:format("Unexpected message~p.~n",[Any])
    after 5000 ->
	io:format("Unable to connect to the transaction server")
    end,
    exit(serverBusy).




process(Window, ServerPid, Transaction) ->
    Transaction_with_No = add_count(Transaction),
    ServerPid ! {request, self()}, %% Send a request to server and wait for proceed message
    d('sent a transaction request message to server'),
    receive
	{proceed, ServerPid} -> send(Window, ServerPid, Transaction_with_No), %% received green light send the transaction.
	d('received a transaction request green light from server');
	{close, ServerPid} -> exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    end.




send(Window, ServerPid, []) ->
    ServerPid ! {confirm, self()}, %% Once all the list (transaction) items sent, send confirmation
    d('sent last chunk of transaction. sent confirm message'),
    receive
	{abort, ServerPid} -> io:format("Aborted... type run if you want to try again!~n");
		       
	{committed, ServerPid} -> io:format("Transaction succeeded!~n");
			  
	{'EXIT', Window, windowDestroyed} -> end_client(ServerPid);
	{close, ServerPid} -> 
	    exit(serverDied);
	Other ->
	    io:format("client active unexpected: ~p~n",[Other])
    end;
send(Window, ServerPid, [H|T]) -> 
    d('sent a chunk of transaction'),
    sleep(4), 
    case loose(5) of
	false -> ServerPid ! {action, self(), H}; 
        true -> ok
    end,
    send(Window, ServerPid, T).



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
    after 200*random:uniform(Latency) ->
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
    io:format("val = ~p~n", [Val]),
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
    %%io:format("client module: ~w~n", [Info]).
    ok.


%%-----------------------------------------------------------
add_count(Transaction) ->
    No = count(Transaction, 0),
    [No|Transaction].

%%-----------------------------------------------------------

%% - Server module
%% - The server module creates a parallel registered process by spawning a process which 
%% evaluates initialize(). 
%% The function initialize() does the following: 
%%      1/ It makes the current process as a system process in order to trap exit.
%%      2/ It creates a process evaluating the store_loop() function.
%%      4/ It executes the server_loop() function.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% The "two phase locking" algorithm is used as the concurrency control
%% mechanism in the server. The philosophy behind this algorithm
%% is to, first, seize the mutexes of all data stores the transaction 
%% is going to manipulate, and then
%% starting the operation. An important point in the algorithm is
%% to seize mutexes of data stores in a predefined order.
%% For each data store, one mutex process is
%% run which is in chagre of controlling access to the data store.
%% One dedicated process listens for incoming clients, and creates
%% a process to interact with the client. So the number of worker
%% processes will be equal to the number of clients that are communicating
%% with server. When a worker process receive the "confirm" message from
%% a client, first it checks the check sum value, and in case it's 
%% correct, considering the data stores it's supposed to manipulate, it
%% tries to seize the relevent mutexes. After completion of transaction,
%% "release" messages are sent to mutexes.

%% There is a probability of transactions
%% arriving at the server incompletely. In such cases, the server asks
%% for those parts that are missing. As soon as it receives the missing 
%% parts, it inserts them into the transaction, and runs the complete
%% transaction.



-module(server).

-export([start/0, inject/3, check_transaction/2, inject_missed_chunks/2, extract/1, extract_count/1]).

%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start() -> 
    register(transaction_server, spawn(fun() ->
					       process_flag(trap_exit, true),
					       Val= (catch initialize()),
					       io:format("Server terminated with:~p~n",[Val])
				       end)).

%%------------------------------------------------------------------------
%% @doc
%% Initializes the server and runs four mutexes which control the four
%% data stores on the server. 
%% @end
%%-----------------------------------------------------------------------
initialize() ->
    process_flag(trap_exit, true),
    Initialvals = [{a,0},{b,0},{c,0},{d,0}], %% All variables are set to 0
    ServerPid = self(),
    StorePid = spawn_link(fun() -> store_loop(ServerPid,Initialvals) end),

    Mutex_a = spawn_link(fun() -> mutex(false) end),
    register(mutex_a, Mutex_a),
    d('mutex_a is running'),
    Mutex_b = spawn_link(fun() -> mutex(false) end),
    register(mutex_b, Mutex_b),
    d('mutex_b is running'),
    Mutex_c = spawn_link(fun() -> mutex(false) end),
    register(mutex_c, Mutex_c),
    d('mutex_c is running'),
    Mutex_d = spawn_link(fun() -> mutex(false) end),
    register(mutex_d, Mutex_d),
    d('mutex_d is running'),
    
    ClientListPid = spawn_link(fun() -> client_loop([]) end),
    server_loop(ClientListPid, StorePid).


    

%%------------------------------------------------------------------------
%% @doc
%% This function doesn't do anything important! Just kept it so that it might
%% be useful for future improvements!
%% @end
%%------------------------------------------------------------------------

client_loop(Clients) ->
    receive
        {login, Client} ->
	    io:format("New client has joined the server:~p.~n", [Client]),
	    client_loop(add_client(Client,Clients));
	{logout, Client} ->
	    io:format("Client~p has left the server.~n", [Client]),
	    client_loop(remove_client(Client, Clients))
    end.
%%%%%%%%%%%%%%%%%%%%%%% STARTING SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% - The server maintains a list of all connected clients and a store holding
%% the values of the global variable a, b, c and d 

%%------------------------------------------------------------------------
%% @doc
%% This functions acts like a listening socket. As soon as a connection request 
%% from a client is recieved, it creates a dedicated process for the client.
%% the dedicated process will be in charge of all future communications
%% with the client.
%% @end
%%-----------------------------------------------------------------------
server_loop(ClientList_loop,StorePid) ->
    d('waiting for a client to connect'),
    receive
	{login, MM, Client} -> 
	    d('client connection request received'),
	    Listener_pid = spawn_link(fun() -> client_individual_listener(ClientList_loop, [], StorePid, false) end),
	    
	    MM ! {ok, Listener_pid},
	    d('sent an ok message to client'),
	    ClientList_loop ! {login, Client},
	    StorePid ! {print},
	    server_loop(ClientList_loop, StorePid);
	Other ->
	    io:format("The main process received the message: ~p~n", [Other]),
	    server_loop(ClientList_loop, StorePid)
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%------------------------------------------------------------------------
%% @doc
%% This functions is in charge of all communications with the client.
%% It receives commands like "request", "confirm" etc, and performs
%% appropriate actions.
%% @end
%%-----------------------------------------------------------------------
client_individual_listener(ClientList_loop, Actions ,StorePid, Count_correct) ->
    receive
	{close, Client} -> 
	    ClientList_loop ! {logout, Client},
	    StorePid ! {print, self()},
	    exit(worker_finished);
	{request, Client} -> 
	    d('received a transaction request from client'),
	    Client ! {proceed, self()},
	    d('sent proceed message to client'),
	    client_individual_listener(ClientList_loop, Actions, StorePid, Count_correct);
	{confirm, Client} -> 
	    d('confirm request received'),
	    Ordered_Actions = lists:sort(Actions),
	    io:format("Transaction: ~p~n", [Ordered_Actions]),
	    {A, Ordered_Actions_Trimmed} = extract_count(Ordered_Actions),
	    
	    io:format("Transaction count: ~p~n", [A]),
	    case A of
		count_missing ->
			io:format("missing trnasaction count!~n"),
			Client ! {count_missing, self()},
			d('noticed the client about the missing count'),
			receive
				{count, New_count} ->
					d('received the new count'),
					Actions_with_count = inject_count_transaction(New_count, Ordered_Actions, []),
					d('the new count was injected'),
					io:format("New transaction after injection: ~p~n", [Actions_with_count]),
					
				
			
				client_individual_listener(ClientList_loop, Actions_with_count, StorePid, Count_correct)
			end;
		0 ->
			Client ! {complete_resend_request, self()},
			io:format("received empty transaction from client. complete resent message sent!~n"),
			receive
					{complete_transaction_again, New_transaction} ->
					io:format("complete transaction again received: ~p~n", [New_transaction]),
					
					client_individual_listener(ClientList_loop, New_transaction, StorePid, false);
					
					Other ->
						io:format("Error .. wrong messagr format received: ~p", [Other])
					
			end;
		_OK ->
	    	case check_transaction(Ordered_Actions_Trimmed, A) of
			[] ->
				d('check count true'),
				Elements = extract(Ordered_Actions_Trimmed),
				Elements_without_duplicates = eliminate_duplicates(Elements),
				Elements_without_duplicates_ordered = lists:sort(Elements_without_duplicates),
				io:format("~p~n", [Elements_without_duplicates_ordered]),
				d('trying to seize locks'),
				case seize_locks(Elements_without_duplicates_ordered) of
			    	all_locks_seized ->
					d('locks seized!'),
					case perform_actions(Ordered_Actions_Trimmed, StorePid) of
						ok ->
							all_locks_released = release_locks(Elements_without_duplicates_ordered),
							StorePid ! {print},
							Client ! {committed, self()},
							client_individual_listener(ClientList_loop, [] ,StorePid, false);
						Other ->
							io:format("Error. could not perform actions. ~p~n", [Other])
					end;
			    	Other ->
					io:format("Error! ~p~n", [Other]),
					Client ! {abort, self()}
				end;
			all_parts_missing ->
				Client ! {complete_resend_request, self()},
				io:format("Only received the transaction count! complete resend message sent!~n"),
				receive
					{complete_transaction_again, New_transaction} ->
					io:format("complete transaction again received: ~p~n", [New_transaction]),
					
					client_individual_listener(ClientList_loop, New_transaction, StorePid, false);
					
					Other ->
						io:format("Error .. wrong messagr format received: ~p", [Other])
					
				end;
			Missing_chunks_no ->
				io:format("chunks missing: ~p~n", [Missing_chunks_no]),
				Client ! {chunks_missing, self(), lists:sort(Missing_chunks_no)},
				receive
					{missed_chunk_resent, Missed_transaction_chunks} ->
					%%io:format("missed chunks received: ~p~n", [Missed_transaction_chunks]),
					Rebuilt_actions = inject_missed_chunks(Missed_transaction_chunks, Ordered_Actions_Trimmed),
					%%io:format("rebuilt transaction: ~p~n", [Rebuilt_actions]),
			
					client_individual_listener(ClientList_loop, [A|Rebuilt_actions], StorePid, true)
				end
	    	end
	    end;
	{action, Client, Act} ->
	    %%io:format("Received~p from client~p.~n", [Act, Client]),
	    client_individual_listener(ClientList_loop, [Act|Actions], StorePid, Count_correct)
    after 200000 ->
	case all_gone(ClientList_loop) of
	    true -> exit(normal);    
	    false -> server_loop(ClientList_loop,StorePid)
	end
    end.

%% - The values are maintained here
%%------------------------------------------------------------------------
%% @doc
%% This function is run on an independent process and manipulates the data store. 
%% @end
%%-----------------------------------------------------------------------
store_loop(ServerPid, Database) -> 
    receive
	{print} -> 
	    io:format("Database status:~n~p.~n",[Database]),
	    store_loop(ServerPid,Database);
	{write, a} ->
	    New_database = write_a(0, Database),
	    store_loop(ServerPid, New_database);
	{write, b} ->
	    New_database = write_b(0, Database),
	    store_loop(ServerPid, New_database);
	{write, c} ->
	    New_database = write_c(0, Database),
	    store_loop(ServerPid, New_database);
	{write, d} ->
	    New_database = write_d(0, Database),
	    store_loop(ServerPid, New_database);
	{write, a, Value} ->
	    New_database = write_a(Value, Database),
	    store_loop(ServerPid, New_database);
	{write, b, Value} ->
	    New_database = write_b(Value, Database),
	    store_loop(ServerPid, New_database);
	{write, c, Value} ->
	    New_database = write_c(Value, Database),
	    store_loop(ServerPid, New_database);
	{write, d, Value} ->
	    New_database = write_d(Value, Database),
	    store_loop(ServerPid, New_database);
	Other ->
	    io:format("store_loop received unknown message of: ~p~n", [Other])
    end.
%%%%%%%%%%%%%%%%%%%%%%% ACTIVE SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%% - Low level function to handle lists
add_client(C,T) -> [C|T].

remove_client(_,[]) -> [];
remove_client(C, [C|T]) -> T;
remove_client(C, [H|T]) -> [H|remove_client(C,T)].

all_gone([]) -> true;
all_gone(_) -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
count([], 0) ->
    true;
count([], _No) ->
    false;
count([_H|T], No) ->
    count(T, No-1).

%%------------------------------------------------------------------------
%% @doc
%% This functions checks the check sum of the received transaction and returns
%% true if the check sum matches the number of commands, otherwise returns false. 
%% @end
%%-----------------------------------------------------------------------
check_count([No|Transaction]) when is_integer(No) ->
    count(Transaction, No);
check_count([No|_Transaction]) when is_tuple(No)->
    false;
check_count([_No|_Transaction]) ->
    false;
check_count([]) ->
    false.

%%------------------------------------------------------------------------
%% @doc
%% This function is the mutex and one instance of it is run for each
%% data store on an independet process. 
%% @end
%%-----------------------------------------------------------------------
mutex(Busy) ->
    receive
	{request, Pid} ->
	    case Busy of
		true ->
		    Pid! {wait},
		    mutex(Busy);
		false ->
		    Pid! {granted},
		    mutex(true)
            end;

        {release, Pid} ->
	    Pid! {released},
            mutex(false)
    end.

%%-----------------------------------------------------------------------


%%------------------------------------------------------------------------
%% @doc
%% This function is used to ask the mutex for exclusive access to the data store. 
%% @end
%%-----------------------------------------------------------------------
wait(a, Pid) ->
    mutex_a! {request, self()},
receive
    {wait} ->
	io:format("mutex_a BUSY .. trying again~n "),
	timer:sleep(random:uniform(10)),
	wait(a, Pid);
    {granted} ->
	%%io:format("mutex_a seizedd!~n"),
	Pid ! {lock_acheived}
end;
wait(b, Pid) ->
    mutex_b! {request, self()},
receive
    {wait} ->
	io:format("mutex_b BUSY .. trying again~n "),
	timer:sleep(random:uniform(10)),
	wait(b, Pid);
    {granted} ->
	%%io:format("mutex_b seizedd!~n"),
	Pid ! {lock_acheived}
end;
wait(c, Pid) ->
    mutex_c! {request, self()},
receive
    {wait} ->
	io:format("mutex_c BUSY .. trying again~n "),
	timer:sleep(random:uniform(10)),
	wait(c, Pid);
    {granted} ->
	%%io:format("mutex_c seizedd!~n"),
	Pid ! {lock_acheived}
end;
wait(d, Pid) ->
    mutex_d! {request, self()},
receive
    {wait} ->
	io:format("mutex_d BUSY .. trying again~n "),
	timer:sleep(random:uniform(10)),
	wait(d, Pid);
    {granted} ->
	%%io:format("mutex_d seizedd!~n"),
	Pid ! {lock_acheived}
end.




%%%%%%%%%%%%%%%%%%%%%%%%
extract(List) ->
    extract(List, []).
extract([], Elements) ->
    Elements;
extract([[_N,{read,a}]|T], Elements) ->
    extract(T, [a|Elements]);
extract([[_N,{read,b}]|T], Elements) ->
    extract(T, [b|Elements]);              
extract([[_N,{read,c}]|T], Elements) ->
    extract(T, [c|Elements]);
extract([[_N,{read,d}]|T], Elements) ->
    extract(T, [d|Elements]);
extract([[_N,{write,a}]|T], Elements) ->
    extract(T, [a|Elements]);
extract([[_N,{write,b}]|T], Elements) ->
    extract(T, [b|Elements]);
extract([[_N,{write,c}]|T], Elements) ->
    extract(T, [c|Elements]);
extract([[_N,{write,d}]|T], Elements) ->
    extract(T, [d|Elements]);
extract([[_N,{write,a,_Value}]|T], Elements) ->
    extract(T, [a|Elements]);
extract([[_N,{write,b,_Value}]|T], Elements) ->
    extract(T, [b|Elements]);
extract([[_N,{write,c,_Value}]|T], Elements) ->
    extract(T, [c|Elements]);
extract([[_N,{write,d,_Value}]|T], Elements) ->
    extract(T, [d|Elements]);
extract([_Count|T], Elements) ->
    io:format("Unknown element in Elements in extract func"),
    extract(T, Elements).

%%----------------------------------------------------------
%%------------------------------------------------------------------------
%% @doc
%% This function negotiates with different mutexes and tries to seize 
%% access to all data stores it receives as argument. 
%% @end
%%-----------------------------------------------------------------------
seize_locks([]) -> 
    all_locks_seized;
seize_locks([a|T]) ->
    ResponsePid = self(),
    Child = spawn(fun() -> wait(a, ResponsePid) end),
    d('request to mutex_a sent'),
    receive
	{lock_acheived} ->
	    d('mutex_a seized'),
	    seize_locks(T);
	Other ->
	    io:format("Wrong message type: ~p~n", [Other])
	after 10000 ->
	    exit(Child, kill),
	    could_not_seize_mutex_a
    end;
seize_locks([b|T]) -> 
    ResponsePid = self(),
    Child = spawn(fun() -> wait(b, ResponsePid) end),
    d('request to mutex_b sent'),
    receive
	{lock_acheived} ->
	    d('mutex_b seized'),
	    seize_locks(T);
	Other ->
	    io:format("Wrong message type: ~p~n", [Other])
	after 10000 ->
	    exit(Child, kill),
	    could_not_seize_mutex_b
    end;
seize_locks([c|T]) ->
    ResponsePid = self(),
    Child = spawn(fun() -> wait(c, ResponsePid) end),
    d('request to mutex_c sent'),
    receive
	{lock_acheived} ->
	    d('mutex_c seized'),
	    seize_locks(T);
	Other ->
	    io:format("Wrong message type: ~p~n", [Other])
	after 10000 ->
	    exit(Child, kill),
	    could_not_seize_mutex_c
    end;
seize_locks([d|T]) ->
    ResponsePid = self(),
    Child = spawn(fun() -> wait(d, ResponsePid) end),
    d('request to mutex_d sent'),
    receive
	{lock_acheived} ->
	    d('mutex_d seized'),
	    seize_locks(T);
	Other ->
	    io:format("Wrong message type: ~p~n", [Other])
	after 10000 ->
	    exit(Child, kill),
	    could_not_seize_mutex_d
    end.

%%-----------------------------------------------------------
%%------------------------------------------------------------------------
%% @doc
%% This function negotiates with mutexes and sends release messages to 
%% the mutexes of all the data stores in receives as argument.
%% @end
%%-----------------------------------------------------------------------
release_locks([]) -> 
    all_locks_released;
release_locks([a|T]) ->
    mutex_a! {release, self()},
    receive
	{released} ->
	    %%io:format("lock a released~n"),
	    release_locks(T)
    end;
release_locks([b|T]) ->
    mutex_b! {release, self()},
    receive
	{released} ->
	    %%io:format("lock b released~n"),
	    release_locks(T)
    end;
release_locks([c|T]) ->
    mutex_c! {release, self()},
    receive
	{released} ->
	    %%io:format("lock c released~n"),
	    release_locks(T)
    end;
release_locks([d|T]) ->
    mutex_d! {release, self()},
    receive
	{released} ->
	    %%io:format("lock d released~n"),
	    release_locks(T)
    end;
release_locks(Other) ->
    io:format("Error: undefined message received: ~p~n", [Other]).

%%-----------------------------------------------------------
%%------------------------------------------------------------------------
%% @doc
%% This function reads through the list of transactions, and through negotiations
%% with "store loop", it accomplishes all actions.
%% @end
%%-----------------------------------------------------------------------
perform_actions([], _StorePid) ->
    ok;
perform_actions([[_N,{read,a}]|T], StorePid) ->
    perform_actions(T, StorePid);
perform_actions([[_N,{read,b}]|T], StorePid) ->
    perform_actions(T, StorePid);
perform_actions([[_N,{read,c}]|T], StorePid) ->
    perform_actions(T, StorePid);
perform_actions([[_N,{read,d}]|T], StorePid) ->
    perform_actions(T, StorePid);
perform_actions([[_N,{write,a}]|T], StorePid) ->
    StorePid ! {write, a},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,b}]|T], StorePid) ->
    StorePid ! {write, b},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,c}]|T], StorePid) ->
    StorePid ! {write, c},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,d}]|T], StorePid) ->
    StorePid ! {write, d},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,a,Value}]|T], StorePid) ->
    StorePid ! {write, a, Value},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,b,Value}]|T], StorePid) ->
    StorePid ! {write, b, Value},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,c,Value}]|T], StorePid) ->
    StorePid ! {write, c, Value},
    perform_actions(T, StorePid);
perform_actions([[_N,{write,d,Value}]|T], StorePid) ->
    StorePid ! {write, d, Value},
    perform_actions(T, StorePid);
perform_actions([_Count|T], StorePid) ->
    %%io:format("perform_actions didnt match any command!~n"),
    perform_actions(T, StorePid).

%%--------------------------------------------------------
write_a(Value, [{a,_V}|T]) ->
    [{a,Value}|T];
write_a(Value, [H|T]) ->
    [H] ++ write_a(Value, T).

%%--------------------------------------------------------
write_b(Value, [{b,_V}|T]) ->
    [{b,Value}|T];
write_b(Value, [H|T]) ->
    [H] ++ write_b(Value, T).

%%--------------------------------------------------------
write_c(Value, [{c,_V}|T]) ->
    [{c,Value}|T];
write_c(Value, [H|T]) ->
    [H] ++ write_c(Value, T).

%%--------------------------------------------------------
write_d(Value, [{d,_V}|T]) ->
    [{d,Value}|T];
write_d(Value, [H|T]) ->
    [H] ++ write_d(Value, T).

%%--------------------------------------------------------
reverse(List) ->
    reverse(List, []).

reverse([], Buffer) ->
    Buffer;
reverse([H|T], Buffer) ->
    reverse(T, [H|Buffer]).

%%---------------------------------------------------------
eliminate_duplicates(List) ->
    eliminate_duplicates(List, []).
eliminate_duplicates([], New_list) ->
    New_list;
eliminate_duplicates([H|T], New_list) ->
    case exists(New_list, H) of
	true ->
	    eliminate_duplicates(T, New_list);
	false ->
	    eliminate_duplicates(T, [H|New_list])
    end.


%%-------------------------------------------------------
exists([], _A) ->
    false;
exists([A|_T], A) ->
    true;
exists([_B|T], A) ->
    exists(T, A).

%%------------------------------------------------------
d(_Info) ->
    %%io:format("Server: ~w~n", [Info]),
    ok.

%%------------------------------------------------------
extract_count([[_N, _Data]|T]) ->
    {count_missing, T};
extract_count([No|T]) ->
    {No, T};
extract_count([]) ->
    {0, zero}.

%%------------------------------------------------------
check_transaction([], A) ->
    all_parts_missing;
check_transaction(Transaction, A) ->
    check_transaction(Transaction, 1, [], A+1).
 
check_transaction(_Transaction, Total_count, Missing_no, Total_count) ->
    Missing_no;

check_transaction([[No, _Data]|T], No, Missing_no, Total_count) -> 
    check_transaction(T, No+1, Missing_no, Total_count);

check_transaction(Transaction, No, Missing_no, Total_count) ->
    check_transaction(Transaction, No+1, [No|Missing_no], Total_count).

%%------------------------------------------------------
inject_missed_chunks([], Transaction) ->
    Transaction;
inject_missed_chunks([Missed_transaction_chunk|T], Transaction) ->
    %%io:format("inject_missed_chunks: missed transaction: ~p Transaction: ~p~n",[Missed_transaction_chunk, Transaction]),
    New_transaction = inject(Missed_transaction_chunk, Transaction, []),
    %%io:format("Transaction after injection: ~p~n", [New_transaction]),
    inject_missed_chunks(T, New_transaction).

%%------------------------------------------------------
inject([Number, Load], [[No, Data]|T], First_part) ->
    case Number == No-1 of
	true ->
		[[Number, Load]] ++ [[No, Data]] ++ First_part ++ T;
	_Other ->
    		case Number == No+1 of
			true ->
				First_part ++ [[No, Data]] ++ [[Number, Load]] ++ T;
			_Other ->
				inject([Number, Load], T, [[No, Data]|First_part])
		end
    end;
inject(_,_,_) ->
    io:format("Error at inject func~n").
    

%%------------------------------------------------------

inject_count_transaction(New_count, Actions, Result) ->
    inject_count_transaction(Actions, [New_count|Result]).

inject_count_transaction([], Result) ->
    Result;
inject_count_transaction([H|T], Result) ->
    inject_count_transaction(T, [H|Result]).



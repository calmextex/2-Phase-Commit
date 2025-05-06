#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (parent_server, parent_server_name) = IpcOneShotServer::new().unwrap();

    // Update the child options to include the IPC path
    child_opts.ipc_path = parent_server_name.clone();

    // Spawn the child process
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    // Create communication channels
    let (parent_tx, parent_rx) = channel().unwrap();

    // Accept the connection from the child
    let (_, (child_tx, child_server_name)): (_, (Sender<ProtocolMessage>, String)) = parent_server.accept().unwrap();

    // Connect to the child's IPC server
    let child_tx0 = Sender::<Sender<ProtocolMessage>>::connect(child_server_name).unwrap();

    // Send the parent's transmitter to the child
    child_tx0.send(parent_tx).unwrap();
    info!("Child process spawned and connected: {:?}", child);

    (child, child_tx, parent_rx)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    //T
    let (child_to_parent_tx, child_to_parent_rx) = channel().unwrap();

    let (child_server, child_server_name) = IpcOneShotServer::new().unwrap();
    
    let coordinator_tx = Sender::<(Sender<ProtocolMessage>, String)>::connect(opts.ipc_path.clone()).unwrap();

    coordinator_tx.send((child_to_parent_tx, child_server_name)).unwrap();

    let (_, coordinator_to_child_tx): (_, Sender<ProtocolMessage>) = child_server.accept().unwrap();
    //info!("Child connected to coordinator: TX = {:?}, RX = {:?}", child_to_parent_tx.clone(), coordinator_to_child_tx);

        
    (coordinator_to_child_tx, child_to_parent_rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    let coordinator_log_path = format!("{}//coordinator.log", opts.log_path);
    info!("Starting the coordinator process.");

    let total_request = opts.num_requests * opts.num_clients;
    
    // Create the coordinator
    let mut coordinator = coordinator::Coordinator::new(
        coordinator_log_path,
        &running,
        total_request
    );

    let mut child_process_list: Vec<(String, Child)> = Vec::new();

    // Spawn clients
    for i in 0..opts.num_clients {
        let mut client_opts = opts.clone();
        client_opts.mode = "client".to_string();
        client_opts.num = i;

        let client_name = format!("client_{}", i);
        let (child_process, client_tx, client_rx) = spawn_child_and_connect(&mut client_opts.clone());
        coordinator.client_join(&client_name, client_tx, client_rx);
        child_process_list.push((client_name, child_process));
    }

    // Spawn participants
    for i in 0..opts.num_participants {
        let mut participant_opts = opts.clone();
        participant_opts.mode = "participant".to_string();
        participant_opts.num = i;

        let participant_name = format!("participant_{}", i);
        let (child_process, participant_tx, participant_rx) = spawn_child_and_connect(&mut participant_opts.clone());
        coordinator.participant_join(&participant_name, participant_tx, participant_rx);
        child_process_list.push((participant_name, child_process));
    }

    // Run the coordinator protocol
    coordinator.protocol();

    // Kill all child processes after the protocol ends
    for (process_name, ref mut child_process) in &mut child_process_list {
        match child_process.kill() {
            Ok(()) => info!("{}::Process killed", process_name),
            Err(e) => warn!("{}::Kill produced an error: {}", process_name, e),
        }
    }

    // Wait for all child processes to exit
    for (process_name, mut child_process) in child_process_list {
        match child_process.wait() {
            Ok(status) => info!("Process {} exited with status: {}", process_name, status),
            Err(e) => eprintln!("Error waiting for process {}: {}", process_name, e),
        }
    }
}

///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    info!("Starting the client process.");

    let (coordinator_tx, client_rx) = connect_to_coordinator(opts);

    let mut client = client::Client::new(
        opts.num.to_string(),
        running,
        coordinator_tx,
        client_rx
    );

    client.protocol(opts.num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
    // Log the start of the participant process
    info!("Starting the participant process with ID: {} and log path: {}", participant_id_str, participant_log_path);

    // Connect to the coordinator and get the communication channels
    let (coordinator_tx, participant_rx) = connect_to_coordinator(opts);

    // Create the participant instance
    let mut participant = participant::Participant::new(
        opts.num.to_string(),
        participant_log_path,
        running,
        opts.send_success_probability,
        opts.operation_success_probability,
        coordinator_tx,
        participant_rx,
        opts.num_requests,
    );

    // Run the participant protocol
    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}

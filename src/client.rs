//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
use message::MessageType;
use message::RequestStatus;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    pub coord_tx: Sender<message::ProtocolMessage>,
    pub child_rx: Receiver<message::ProtocolMessage>,
    pub success_count: u64,
    pub failure_count: u64,
    pub unknown_count: u64,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(
        client_id: String,
        running: Arc<AtomicBool>,
        coord_tx: Sender<message::ProtocolMessage>,
        child_rx: Receiver<message::ProtocolMessage>,
    ) -> Client {
        Client {
            id_str: format!("client_{}", client_id),
            running,
            num_requests: 0,
            coord_tx,
            child_rx,
            success_count: 0,
            failure_count: 0,
            unknown_count: 0,
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());
    
        while self.running.load(Ordering::SeqCst) {
        let result = match self.child_rx.try_recv() {
            Ok(message) => message,
            Err(error) => match error {
                TryRecvError::Empty => {
                    warn!(
                        "{}::No message received from coordinator, error: {:#?}",
                        self.id_str,
                        error
                    );
                    thread::sleep(Duration::from_millis(5)); 
                    continue; 
                }
                TryRecvError::IpcError(ipc_channel::ipc::IpcError::Disconnected) => {
                    warn!(
                        "{}::Disconnected from coordinator, error: {:#?}",
                        self.id_str,
                        error
                    );
                    thread::sleep(Duration::from_millis(5)); 
                    continue; 
                }
                TryRecvError::IpcError(_) => {
                    error!(
                        "{}::Unexpected IPC error from coordinator, error: {:#?}",
                        self.id_str,
                        error
                    );
                    panic!("{:#?}", error);
                }
            },
        };

        if result.mtype == message::MessageType::CoordinatorExit {
            info!("{}::Received exit signal from coordinator", self.id_str.clone());
            break;
        }
    }
    
        trace!("{}::Exiting", self.id_str.clone());
    }
    
    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.num_requests);
        info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);

        // TODO
        // Send the message to the coordinator (via a channel)
        if let Err(e) = self.coord_tx.send(pm) {
            error!(
                "{}::Failed to send operation #{}: {:?}",
                self.id_str.clone(),
                self.num_requests,
                e
            );
            // If send fails, terminate the client
            panic!("{}::Terminating client due to failed send", self.id_str.clone());
        }

        trace!("{}::Sent operation #{}", self.id_str.clone(), self.num_requests);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self) {
        match self.child_rx.recv() {
            Ok(message) => {
                match message.mtype {
                    message::MessageType::ClientResultCommit => {
                        self.success_count += 1;
                        trace!("{}::Transaction committed: {}", self.id_str.clone(), message.txid);
                    }
                    message::MessageType::ClientResultAbort => {
                        self.failure_count += 1;
                        if message.txid.contains("unknown") {
                            self.unknown_count += 1;
                        }
                        trace!("{}::Transaction aborted: {}", self.id_str.clone(), message.txid);
                    }
                    _ => {
                        warn!(
                            "{}::Unexpected message type received: {:?}",
                            self.id_str.clone(),
                            message.mtype
                        );
                    }
                }
            }
            Err(e) => {
                error!(
                    "{}::Failed to receive result: {:?}",
                    self.id_str.clone(),
                    e
                );
                panic!("{}::Terminating client due to failed receive", self.id_str.clone());
            }
        }
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        let success_count: u64 = self.success_count;
        let failure_count: u64 = self.failure_count;
        let unknown_count: u64 = self.unknown_count;

        println!(
            "{:16}:\t Committed: {:6}\tAborted: {:6}\tUnknown: {:6}",
            self.id_str.clone(),
            self.success_count,
            self.failure_count,
            self.unknown_count
        );
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {
        for _ in 0..n_requests {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            self.send_next_operation();
            self.recv_result();
        }
        //self.wait_for_exit_signal();
        self.report_status();
    }
}

//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;


use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
    SentGlobalDecision,
    CoordinatorExit
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    max_requests: u32,                
    successful_ops: u64,       
    failed_ops: u64,           
    unknown_ops: u64,
    client_connections: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>, 
    participant_connections: HashMap<String, (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,   
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>, max_requests: u32) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            // TODO
            max_requests,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            client_connections: HashMap::new(),
            participant_connections: HashMap::new(),
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, sender: Sender<ProtocolMessage>, receiver: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        self.participant_connections.insert(name.clone(), (sender, receiver));
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, sender: Sender<ProtocolMessage>, receiver: Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        self.client_connections.insert(name.clone(), (sender, receiver));
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = self.successful_ops;
        let failed_ops: u64 = self.failed_ops;
        let unknown_ops: u64 = self.unknown_ops;

        println!(
            "coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", 
            successful_ops, failed_ops, unknown_ops
        );
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        //TODO
        let mut processed_requests = 0;

        while self.running.load(Ordering::SeqCst) && processed_requests <= self.max_requests {
            if processed_requests == self.max_requests {
                let exit_message = message::ProtocolMessage::instantiate(
                    MessageType::CoordinatorExit,
                    0,
                    String::new(),
                    String::new(),
                    0,
                );

                for (_, (sender, _)) in self.participant_connections.iter() {
                    let _ = sender.send(exit_message.clone());
                }
                thread::sleep(Duration::from_millis(5));
                for (_, (sender, _)) in self.client_connections.iter() {
                    let _ = sender.send(exit_message.clone());
                }

                self.log.append(
                    MessageType::CoordinatorExit,
                    String::new(),
                    String::new(),
                    0,
                );
                break;
            }

            for (client_id, (client_sender, client_receiver)) in self.client_connections.iter() {
                match client_receiver.try_recv() {
                    Ok(request_message) => {
                        // Log initial request
                        self.state = CoordinatorState::ReceivedRequest;
                        self.log.append(
                            request_message.mtype,
                            request_message.txid.clone(),
                            request_message.senderid.clone(),
                            request_message.opid,
                        );

                        // Prepare phase
                        let mut participant_message_phase1 = request_message.clone();
                        participant_message_phase1.mtype = MessageType::CoordinatorPropose;
                        self.state = CoordinatorState::ProposalSent;
                        self.log.append(
                            participant_message_phase1.mtype,
                            participant_message_phase1.txid.clone(),
                            participant_message_phase1.senderid.clone(),
                            participant_message_phase1.opid,
                        );

                        // Send to all participants
                        let total_participants = self.participant_connections.len();
                        for (_, (participant_sender, _)) in self.participant_connections.iter() {
                            let _ = participant_sender.send(participant_message_phase1.clone());
                        }

                        // Collect votes with timeout
                        let mut received_votes = 0;
                        let mut commit_votes = 0;
                        let timeout_duration = Duration::from_millis(50);
                        let start = std::time::Instant::now();
                        let mut vote_timed_out = false;

                        while received_votes < total_participants && !vote_timed_out {
                            if start.elapsed() >= timeout_duration {
                                vote_timed_out = true;
                                break;
                            }

                            for (_, (_, participant_receiver)) in self.participant_connections.iter() {
                                match participant_receiver.try_recv() {
                                    Ok(vote) => {
                                        received_votes += 1;
                                        if vote.mtype == MessageType::ParticipantVoteCommit {
                                            commit_votes += 1;
                                        }
                                        self.log.append(
                                            vote.mtype,
                                            vote.txid.clone(),
                                            vote.senderid.clone(),
                                            vote.opid,
                                        );
                                    }
                                    Err(TryRecvError::Empty) => {
                                        thread::sleep(Duration::from_millis(5));
                                        continue;
                                    }
                                    Err(_) => continue,
                                }
                            }
                        }

                        // Make decision
                        let (decision_type, client_type) = if commit_votes == total_participants && !vote_timed_out {
                            self.successful_ops += 1;
                            (MessageType::CoordinatorCommit, MessageType::ClientResultCommit)
                        } else {
                            self.failed_ops += 1;
                            if vote_timed_out {
                                self.unknown_ops += 1;
                            }
                            (MessageType::CoordinatorAbort, MessageType::ClientResultAbort)
                        };

                        // Send decision to all participants
                        let mut final_message = request_message.clone();
                        final_message.mtype = decision_type;
                        self.log.append(
                            final_message.mtype,
                            final_message.txid.clone(),
                            final_message.senderid.clone(),
                            final_message.opid,
                        );
                        
                        for (_, (participant_sender, _)) in self.participant_connections.iter() {
                            let _ = participant_sender.send(final_message.clone());
                        }

                        // Send result to client
                        let mut client_message = request_message;
                        client_message.mtype = client_type;
                        let _ = client_sender.send(client_message);

                        processed_requests += 1;
                    }
                    Err(_) => continue,
                }
            }
        }

        self.report_status();
    }
}

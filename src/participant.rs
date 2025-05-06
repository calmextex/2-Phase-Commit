
//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;
use participant::ipc_channel::ipc::IpcError;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedCommit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
    max_requests: u32,
    coordinator_channel: Sender<ProtocolMessage>,
    participant_channel: Receiver<ProtocolMessage>,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        coordinator_channel: Sender<ProtocolMessage>,
        participant_channel: Receiver<ProtocolMessage>,
        max_requests: u32,) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            // TODO
            coordinator_channel,
            participant_channel,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            max_requests,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        if x <= self.send_success_prob {
            // TODO: Send success
            match self.coordinator_channel.send(pm) {
                Ok(()) => (),
                Err(error) => {
                    error!("{}::Failure to send", self.id_str);
                }
            }
        } else {
            // TODO: Send fail
            eprintln!("{}::Send failed due to probability check", self.id_str);
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: &Option<ProtocolMessage>) -> bool {

        trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x <= self.operation_success_prob {
            // Successful operation: set mtype to ParticipantVoteCommit
            let mut request_message = request_option.clone().unwrap();
            request_message.mtype = MessageType::ParticipantVoteCommit;
            self.log.append(
                request_message.mtype,
                request_message.txid,
                request_message.senderid,
                request_message.opid,
            );
            trace!("{}::Operation succeeded", self.id_str.clone());
            
        } else {
            // Failed operation: set mtype to ParticipantVoteAbort
            let mut request_message = request_option.clone().unwrap();
            request_message.mtype = MessageType::ParticipantVoteAbort;
            self.log.append(
                request_message.mtype,
                request_message.txid,
                request_message.senderid,
                request_message.opid,
            );
            trace!("{}::Operation failed", self.id_str.clone());
            return false;
        }
        true
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

        println!("participant_{:16}:\t Committed: {:6}\tAborted: {:6}\tUnknown: {:6}", 
            self.id_str.clone(),
            successful_ops,
            failed_ops,
            unknown_ops
        );
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        while self.running.load(Ordering::SeqCst) {
            match self.participant_channel.try_recv() {
                Ok(protocol_message) => {
                    if protocol_message.mtype == MessageType::CoordinatorExit {
                        info!("{}::Received exit signal from coordinator", self.id_str);
                        break;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // No message received; sleep and retry
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => {
                    // Handle unexpected errors
                    error!(
                        "{}::Error while waiting for exit signal: {:#?}",
                        self.id_str, error
                    );
                    break;
                }
            }
        }
        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        trace!("{}::protocol start", self.id_str);

        while self.running.load(Ordering::SeqCst) {
            // Receive a protocol message from the coordinator
            match self.participant_channel.recv() {
                Ok(protocol_message) => match protocol_message.mtype {
                    MessageType::CoordinatorPropose => {
                        self.state = ParticipantState::ReceivedP1;
                        self.unknown_ops += 1;

                        // Perform the operation and decide to commit or abort
                        let vote_type = if self.perform_operation(&Some(protocol_message.clone())) {
                            self.state = ParticipantState::VotedCommit;
                            MessageType::ParticipantVoteCommit
                        } else {
                            self.state = ParticipantState::VotedAbort;
                            MessageType::ParticipantVoteAbort
                        };

                        // Generate the vote message and send it to the coordinator
                        let vote_message = ProtocolMessage::generate(
                            vote_type,
                            protocol_message.txid.clone(),
                            self.id_str.clone(),
                            protocol_message.opid,
                        );

                        self.send(vote_message);
                        self.state = ParticipantState::AwaitingGlobalDecision;
                    }
                    MessageType::CoordinatorCommit => {
                        self.state = ParticipantState::Quiescent;
                        self.successful_ops += 1;
                        self.unknown_ops -= 1;

                        // Log the commit decision
                        self.log.append(
                            protocol_message.mtype,
                            protocol_message.txid.clone(),
                            protocol_message.senderid.clone(),
                            protocol_message.opid,
                        );
                    }
                    MessageType::CoordinatorAbort => {
                        self.state = ParticipantState::Quiescent;
                        self.failed_ops += 1;
                        self.unknown_ops -= 1;

                        // Log the abort decision
                        self.log.append(
                            protocol_message.mtype,
                            protocol_message.txid.clone(),
                            protocol_message.senderid.clone(),
                            protocol_message.opid,
                        );
                    }
                    MessageType::CoordinatorExit => {
                        info!("{}::Received exit signal", self.id_str);
                        break;
                    }
                    _ => unreachable!(),
                },
                Err(_) => thread::sleep(Duration::from_millis(5)), // Retry on message receive failure
            }
        }

        // Exit signal received; clean up and report status
        //self.wait_for_exit_signal();
        self.report_status();
    }

}


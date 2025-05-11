#![allow(unused)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bdk_chain::bitcoin;
use bdk_chain::{Anchor, BlockId, ChainPosition, TxGraph, tx_graph::CanonicalTx};
use bitcoin::{OutPoint, Transaction, TxIn, TxOut, Txid};

/*
Say we have a pool of txs represented by a dependency graph
  (B spends A, E spends D, etc)

 A   C
/ \ /
B  D
  / \
 E   F

You can imagine a possible grouping of the txs
    [(A, B), (C, D, E), (F)]
*/

// Level 1: Audit the pool

/// Package selector
#[derive(Debug)]
pub struct PackageSelector<A> {
    // Entries by txid
    map: HashMap<Txid, Entry<A>>,
    // Transaction graph
    graph: TxGraph<A>,
}

/// An entry in a pool of transactions to be sorted
#[derive(Debug, Clone)]
struct Entry<A> {
    txid: Txid,
    chain_position: Option<ChainPosition<A>>,
    tx: Arc<Transaction>,
    ancestors: HashSet<Txid>,
    descendants: HashSet<Txid>,
}

impl<A: Anchor> PackageSelector<A> {
    /// New from an iterator of canonical txs
    pub fn from_canonical_txs<'a, I>(txs: I) -> Self
    where
        I: IntoIterator<Item = CanonicalTx<'a, Arc<Transaction>, A>>,
        A: 'a,
    {
        Self::from_chain_position_txs(
            txs.into_iter()
                .map(|c| (c.tx_node.tx, Some(c.chain_position))),
        )
    }

    /// New from an iterator of txs
    pub fn new<I, T>(txs: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<Arc<Transaction>>,
    {
        Self::from_chain_position_txs(txs.into_iter().map(|tx| (tx.into(), None)))
    }

    /// New from tuples of (tx, chain position)
    pub fn from_chain_position_txs<I>(txs: I) -> Self
    where
        I: IntoIterator<Item = (Arc<Transaction>, Option<ChainPosition<A>>)>,
    {
        let txs = txs.into_iter().collect::<Vec<_>>();

        // Populate tx graph
        let mut graph = TxGraph::default();
        for (tx, _) in &txs {
            let _ = graph.insert_tx(tx.clone());
        }

        let map = txs
            .into_iter()
            .map(|(tx, chain_position)| {
                let entry = Entry {
                    txid: tx.compute_txid(),
                    chain_position,
                    tx,
                    ancestors: HashSet::default(),
                    descendants: HashSet::default(),
                };

                (entry.txid, entry)
            })
            .collect::<HashMap<_, _>>();

        let mut selector = Self { map, graph };

        // Populate ancestor/descendant sets
        selector.set_links();

        selector
    }

    /// Fill in the ancestor and descendant sets for all entries. Note that each set
    /// includes the entry itself.
    fn set_links(&mut self) {
        for (&txid, entry) in self.map.iter_mut() {
            let ancestors = &mut entry.ancestors;
            ancestors.insert(entry.txid);
            ancestors.extend(
                self.graph
                    .walk_ancestors(entry.tx.clone(), |_, tx| Some(tx.compute_txid())),
            );

            let descendants = &mut entry.descendants;
            descendants.insert(entry.txid);
            descendants.extend(self.graph.walk_descendants(txid, |_, txid| Some(txid)));
        }
    }

    // Level 2: Select packages

    /// Select groups of related transactions into non-overlapping packages.
    ///
    /// A package is defined as a single tx and all of its not-yet-selected ancestors.
    /// Packages are created and returned in order of chain position of the terminal child.
    pub fn select(&self) -> Vec<Vec<Txid>> {
        let mut visited = HashSet::new();
        let mut ret = vec![];

        // Locate the terminal children
        let mut children = self
            .map
            .values()
            .filter(|e| e.descendants.len() == 1)
            .collect::<Vec<_>>();
        children.sort_by_key(|e| e.chain_position.as_ref());

        for entry in children {
            // Create package
            let mut package = vec![];
            for txid in &entry.ancestors {
                let entry = self.map.get(txid).expect("entry must exist");
                if visited.insert(entry.txid) {
                    package.push(entry);
                }
            }
            // Sort package txs topologically
            package.sort_by_key(|e| e.ancestors.len());

            let package: Vec<Txid> = package.into_iter().map(|e| e.txid).collect();
            ret.push(package);
        }

        ret
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // L1 demo: audit txs
    let tx_1 = Transaction {
        input: vec![TxIn::default()],
        output: vec![TxOut::NULL],
        ..new_tx(0)
    };
    let txid_1 = tx_1.compute_txid();
    let tx_2 = Transaction {
        input: vec![TxIn {
            previous_output: OutPoint::new(txid_1, 0),
            ..Default::default()
        }],
        output: vec![TxOut::NULL],
        ..new_tx(1)
    };
    let txid_2 = tx_2.compute_txid();
    let tx_3 = Transaction {
        input: vec![TxIn {
            previous_output: OutPoint::new(txid_2, 0),
            ..Default::default()
        }],
        output: vec![TxOut::NULL],
        ..new_tx(2)
    };

    let txs = [Arc::new(tx_1), Arc::new(tx_2), Arc::new(tx_3)];
    let exp_txids = txs.iter().map(|tx| tx.compute_txid()).collect::<Vec<_>>();
    for &txid in exp_txids.iter() {
        println!("Txid {}", txid);
    }

    let selector = PackageSelector::<BlockId>::new(txs);
    // dbg!(&selector.map);

    // L2 demo: select packages
    let packages: Vec<Vec<Txid>> = selector.select();
    assert_eq!(packages.len(), 1);

    assert_eq!(&packages[0], &exp_txids[..]);

    println!("Package: {:#?}", packages[0]);

    Ok(())
}

fn new_tx(lt: u32) -> Transaction {
    Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::from_consensus(lt),
        input: vec![],
        output: vec![],
    }
}

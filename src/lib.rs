use std::sync::Arc;

use bdk_chain::bitcoin;
use bdk_chain::{Anchor, ChainPosition, TxGraph, tx_graph::CanonicalTx};
use bitcoin::{Transaction, Txid};

use std::collections::{HashMap, HashSet};

/// Package selector
#[derive(Debug)]
pub struct PackageSelector<A> {
    // entries by txid
    map: HashMap<Txid, Entry<A>>,
    // tx graph
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
                let entry = self.map.get(txid).expect("oops: missing Entry");
                if visited.insert(entry.txid) {
                    package.push(entry);
                }
            }
            // Sort package txs topologically
            package.sort_by_key(|e| e.ancestors.len());

            let package = package.into_iter().map(|e| e.txid).collect();
            ret.push(package);
        }

        ret
    }
}

#[cfg(test)]
#[allow(unused)]
mod test {
    use super::*;

    use bdk_chain::local_chain::LocalChain;
    use bdk_chain::{CanonicalizationParams, ConfirmationBlockTime};
    use bitcoin::secp256k1::rand;
    use bitcoin::{OutPoint, TxIn, TxOut, constants};
    use rand::seq::SliceRandom;

    fn new_tx(lt: u32) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version(1),
            lock_time: bitcoin::absolute::LockTime::from_consensus(lt),
            input: vec![],
            output: vec![],
        }
    }

    #[test]
    fn sort_canonical_txs() {
        /* We have a group of related txs
            B spends A
            D spends A and C
            E and F spend D
                A   C
               / \ /
              B   D
                 / \
                E   F

        we expect three packages:
            (A, B),
            (C, D, E),
            (F),

        */
        let tx_a = Transaction {
            input: vec![TxIn::default()],
            output: vec![TxOut::NULL, TxOut::NULL],
            ..new_tx(0)
        };
        let txid_a = tx_a.compute_txid();
        let tx_b = Transaction {
            input: vec![TxIn {
                previous_output: OutPoint::new(txid_a, 0),
                ..Default::default()
            }],
            output: vec![TxOut::NULL],
            ..new_tx(1)
        };
        let tx_c = Transaction {
            input: vec![TxIn::default()],
            output: vec![TxOut::NULL],
            ..new_tx(2)
        };
        let txid_c = tx_c.compute_txid();
        let tx_d = Transaction {
            input: vec![
                TxIn {
                    previous_output: OutPoint::new(txid_a, 1),
                    ..Default::default()
                },
                TxIn {
                    previous_output: OutPoint::new(txid_c, 0),
                    ..Default::default()
                },
            ],
            output: vec![TxOut::NULL, TxOut::NULL],
            ..new_tx(3)
        };
        let txid_d = tx_d.compute_txid();
        let tx_e = Transaction {
            input: vec![TxIn {
                previous_output: OutPoint::new(txid_d, 0),
                ..Default::default()
            }],
            output: vec![TxOut::NULL],
            ..new_tx(4)
        };
        let tx_f = Transaction {
            input: vec![TxIn {
                previous_output: OutPoint::new(txid_d, 1),
                ..Default::default()
            }],
            output: vec![TxOut::NULL],
            ..new_tx(5)
        };
        let txs = [
            Arc::new(tx_a),
            Arc::new(tx_b),
            Arc::new(tx_c),
            Arc::new(tx_d),
            Arc::new(tx_e),
            Arc::new(tx_f),
        ];

        let exp_txids = txs.iter().map(|tx| tx.compute_txid()).collect::<Vec<_>>();

        let mut graph = TxGraph::<ConfirmationBlockTime>::default();
        for tx in txs {
            let _ = graph.insert_tx(tx);
        }
        for (seen_at, &txid) in (1..7).zip(&exp_txids) {
            let _ = graph.insert_seen_at(txid, seen_at);
        }

        let genesis_hash = constants::genesis_block(bitcoin::Network::Bitcoin).block_hash();
        let (chain, _) = LocalChain::from_genesis_hash(genesis_hash);

        let canon_txs = graph
            .list_canonical_txs(
                &chain,
                chain.tip().block_id(),
                CanonicalizationParams::default(),
            )
            .collect::<Vec<_>>();

        assert_eq!(canon_txs.len(), 6);

        let selector = PackageSelector::from_canonical_txs(canon_txs);
        let sorted: Vec<Vec<Txid>> = selector.select();
        assert_eq!(sorted.len(), 3);
        assert_eq!(&sorted[0], &exp_txids[0..2]); // A, B
        assert_eq!(&sorted[1], &exp_txids[2..5]); // C, D, E
        assert_eq!(&sorted[2], &exp_txids[5..]); // F
    }

    fn create_spends(tx: Transaction, n: u32) -> Vec<Transaction> {
        let mut previous_output = OutPoint::new(tx.compute_txid(), 0);

        let mut txs = vec![tx];

        for i in 1..n {
            let tx = Transaction {
                input: vec![TxIn {
                    previous_output,
                    ..Default::default()
                }],
                output: vec![TxOut::NULL],
                ..new_tx(i)
            };
            previous_output = OutPoint::new(tx.compute_txid(), 0);
            txs.push(tx);
        }

        txs
    }

    #[test]
    fn sort_single_package() {
        // Chain `TX_COUNT` dependent txs. We expect a single sorted package.
        const TX_COUNT: u32 = 16;
        // const TX_COUNT: u32 = 64;

        let tx_a = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![TxIn::default()],
            output: vec![TxOut::NULL],
        };

        let mut txs: Vec<Transaction> = create_spends(tx_a, TX_COUNT);

        assert_eq!(txs.len(), TX_COUNT as usize);

        let exp_txids = txs.iter().map(|tx| tx.compute_txid()).collect::<Vec<_>>();

        // Shuffle the original order
        txs.shuffle(&mut rand::thread_rng());

        let selector = PackageSelector::<ConfirmationBlockTime>::new(txs);
        let sorted: Vec<Vec<Txid>> = selector.select();
        assert_eq!(sorted.len(), 1);
        assert_eq!(&sorted[0], &exp_txids);
    }
}

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

fn main() {
    println!("Hello, world!");
}

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

use crate::column_metadata::ColumnMetadata;
use crate::mpc_view::MPCView;
use petgraph::graph::DiGraph;

pub struct Dag<T: ColumnMetadata> {
    graph: DiGraph<T, usize>,
    sorted_columns: Vec<T>,
    current_index: usize,
}

impl<T: ColumnMetadata> Dag<T> {
    pub fn next_column(&mut self) -> Option<&T> {
        let node = self.sorted_columns.get(self.current_index);
        self.current_index += 1;
        node
    }

    pub fn next_columns(&mut self) -> Option<Vec<&T>> {
        unimplemented!("Support for per-level operation not implemented")
    }

    pub fn reset(&mut self) {
        self.current_index = 0;
    }

    pub fn from_mpc_view(mpc_view: MPCView<T>) -> Self {
        let graph = Self::build_graph(
            &mpc_view.input_columns,
            &mpc_view.helper_columns,
            &mpc_view.metrics,
        );

        let sorted_nodes = Self::toposort_nodes(&graph);

        Self {
            graph,
            sorted_columns: sorted_nodes,
            current_index: 0,
        }
    }

    fn build_graph(input_columns: &[T], helper_columns: &[T], metrics: &[T]) -> DiGraph<T, usize> {
        let mut graph = DiGraph::new();

        // 1. add the metric column data to the graph
        // 2. insert a mapping from node -> graph index
        // 3. repeat for every column
        let node_to_index = input_columns
            .iter()
            .chain(helper_columns.iter().chain(metrics.iter()))
            .fold(std::collections::HashMap::new(), |mut acc, &node| {
                let i = graph.add_node(node.clone());
                acc.insert(node, i);
                acc
            });

        for to_node in input_columns
            .iter()
            .chain(helper_columns.iter().chain(metrics.iter()))
        {
            let to_index = node_to_index
                .get(to_node)
                .unwrap_or_else(|| panic!("Column {} was not found in the graph", to_node.name()));
            for from_node in to_node.dependencies().iter() {
                let from_index = node_to_index.get(from_node).unwrap_or_else(|| {
                    panic!("Column {} was not found in the graph", from_node.name())
                });
                // 0 is the weight
                graph.add_edge(*from_index, *to_index, 0);
            }
        }

        graph
    }

    fn toposort_nodes(graph: &DiGraph<T, usize>) -> Vec<T> {
        match petgraph::algo::toposort(&graph, None) {
            Ok(order) => order
                .into_iter()
                .map(|node_index| *graph.node_weight(node_index).unwrap())
                .collect(),
            Err(_e) => panic!("Cycle detected in graph"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column_metadata;
    use crate::column_metadata::ColumnMetadata;
    use crate::dag::Dag;
    use crate::mpc_metric_dtype::MPCMetricDType;
    use crate::mpc_view::MPCView;
    use crate::row::Row;

    column_metadata! {
        TestEnum {
            Variant1 -> [],
            Variant2 -> [Variant1],
            Variant3 -> [Variant1],
            Variant4 -> [Variant2, Variant3],
        }
    }

    impl TestEnum {
        fn from_row(&self, _r: &Row<Self>) -> MPCMetricDType {
            panic!("Undefined for test");
        }
        fn aggregate<I: Iterator<Item = Row<Self>>>(&self, _rows: I) -> MPCMetricDType {
            panic!("Undefined for test");
        }
    }

    fn get_mpc_view() -> MPCView<TestEnum> {
        MPCView::new(
            vec![TestEnum::Variant1],
            vec![TestEnum::Variant2, TestEnum::Variant3],
            vec![TestEnum::Variant4],
            vec![],
        )
    }

    #[test]
    fn dag_next_node() {
        let mpc_view = get_mpc_view();
        let mut dag = Dag::from_mpc_view(mpc_view);
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant1));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant3));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant2));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant4));
        assert_eq!(dag.next_column(), None);
        dag.reset();
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant1));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant3));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant2));
        assert_eq!(dag.next_column(), Some(&TestEnum::Variant4));
    }
}

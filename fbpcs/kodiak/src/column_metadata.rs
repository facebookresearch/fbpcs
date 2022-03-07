pub trait ColumnMetadata {
    fn name(&self) -> String;
    fn dependencies(&self) -> Vec<Self>
}

trait DbSized{
    const SIZE:u32;
}
trait Seralize{
    fn to_bytes(&self)->Vec<u8>;
    fn from_bytes(&mut self,bytes: Vec<u8>);
}
impl Seralize for Row{
    fn to_bytes(&self)->Vec<u8>{

    }

}
struct Row{
    test_data:u32

}
struct DB{
    data: Vec<Row>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

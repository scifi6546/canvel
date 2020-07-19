use std::collections::HashMap;
use std::fs::File;
use std::io::SeekFrom;
use std::io::{Read, Seek, Write};
use std::ops::Index;
pub type Id = u32;
pub trait Serialize {
    fn size_of() -> usize;
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Self;
}
pub struct Block<Row: Serialize + Clone> {
    //Block structure
    //```
    //
    //FILE START
    //   .
    //   .
    //(get_block_size)*index start:   rows
    //BIT_MAP
    //ROWS
    //(get_block_size)*(index+1)-size_of: last row
    data: HashMap<Id, Row>,
    index_start: Id, //index of smallest element
}
impl<Row: Serialize + Clone + std::marker::Sized> Index<(&File, &Id)> for Block<Row> {
    type Output = Row;
    fn index(&self, i: (&File, &Id)) -> &Self::Output {
        self.get(i.0, i.1.clone())
    }
}
impl<Row: Serialize + Clone + std::marker::Sized> Block<Row> {
    pub const BLOCK_SIZE: u32 = 4096;
    fn get_bitmap_size() -> usize {
        let mut bitmap_size: usize = Self::BLOCK_SIZE as usize / 8;
        //checking if last bitmap has unused elements
        if Self::BLOCK_SIZE % 8 != 0 {
            bitmap_size += 1;
        }
        return bitmap_size;
    }
    pub fn get_block_size() -> usize {
        let bitmap_size: usize = Self::get_bitmap_size();
        return bitmap_size + (Self::BLOCK_SIZE as usize) * Row::size_of();
    }
    fn get_file_start(&self) -> usize {
        (self.index_start / Self::BLOCK_SIZE) as usize * Self::get_block_size()
    }

    pub fn get(&self, _file: &File, id: Id) -> &Row {
        self.data.get(&id).unwrap()
    }
    pub fn contains(&self, _file: &File, id: Id) -> bool {
        self.data.contains_key(&id)
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    //removes id
    pub fn remove(&mut self, file: &mut File, id: Id) {
        self.data.remove(&id);
        //first part make bitmap

        let mut bit = 0;
        //iterating through part of bitmap
        //lower index is least significant bit
        let start = id - id % 8;
        if self.data.contains_key(&start) {
            bit += 1;
        }
        if self.data.contains_key(&(start + 1)) {
            bit += 0b10;
        }
        if self.data.contains_key(&(start + 2)) {
            bit += 0b100;
        }
        if self.data.contains_key(&(start + 3)) {
            bit += 0b1000;
        }
        if self.data.contains_key(&(start + 4)) {
            bit += 0b1_0000;
        }
        if self.data.contains_key(&(start + 5)) {
            bit += 0b10_0000;
        }
        if self.data.contains_key(&(start + 6)) {
            bit += 0b100_0000;
        }
        if self.data.contains_key(&(start + 7)) {
            bit += 0b1000_0000;
        }
        file.seek(SeekFrom::Start(
            self.get_file_start() as u64 + id as u64 / 8,
        ))
        .ok()
        .unwrap();
        file.write(&[bit]);
    }
    //insert row into db
    pub fn insert(&mut self, file: &mut File, id: Id, data: Row) {
        self.data.insert(id, data.clone());
        //first part make bitmap

        let mut bit = 0;
        //iterating through part of bitmap
        //lower index is least significant bit
        let start = id - id % 8;
        if self.data.contains_key(&start) {
            bit += 1;
        }
        if self.data.contains_key(&(start + 1)) {
            bit += 0b10;
        }
        if self.data.contains_key(&(start + 2)) {
            bit += 0b100;
        }
        if self.data.contains_key(&(start + 3)) {
            bit += 0b1000;
        }
        if self.data.contains_key(&(start + 4)) {
            bit += 0b1_0000;
        }
        if self.data.contains_key(&(start + 5)) {
            bit += 0b10_0000;
        }
        if self.data.contains_key(&(start + 6)) {
            bit += 0b100_0000;
        }
        if self.data.contains_key(&(start + 7)) {
            bit += 0b1000_0000;
        }
        file.seek(SeekFrom::Start(
            self.get_file_start() as u64 + id as u64 / 8,
        ))
        .ok()
        .unwrap();
        file.write(&[bit]);

        //Next get index of data
        let row_index =
            self.get_file_start() + Self::get_bitmap_size() + Row::size_of() * id as usize;
        file.seek(SeekFrom::Start(row_index as u64));
        file.write(&data.to_bytes());
        //procedure: Figure out right part of file and then write bytes to it
        // and then insert data into hashmap
    }
    fn load_from_disk(file: &mut File, id_start: Id) -> Self {
        //first load bitmap
        let mut hash = HashMap::new();
        let mut bitmap = vec![0; Self::get_bitmap_size()];
        let len = file.read(&mut bitmap).unwrap();
        assert_eq!(len, Self::get_bitmap_size());
        let mut row_data = vec![0; Self::BLOCK_SIZE as usize * Row::size_of()];
        let len = file.read(&mut row_data).unwrap();
        assert_eq!(len, Self::BLOCK_SIZE as usize * Row::size_of());
        for i in 0..Self::BLOCK_SIZE {
            let byte = bitmap[i as usize / 8];
            let present = (byte >> i % 8) & 0b01;
            if present == 1 {
                //if the row is present add it to hashmap
                let mut data: Vec<u8> = vec![];
                data.reserve(Row::size_of());
                for j in 0..Row::size_of() {
                    data.push(row_data[i as usize * Row::size_of() + j]);
                }
                hash.insert(i + id_start, Row::from_bytes(data));
            }
        }
        return Block {
            data: hash,
            index_start: id_start,
        };
        //next load proper things from data segment
        //return block
    }
    //creates new zero element file
    pub fn new(file: &mut File, id_start: Id) -> Self {
        let hash = HashMap::new();
        let mut bitmap = vec![0; Self::get_bitmap_size()];
        let len = file.write(&mut bitmap).unwrap();
        assert_eq!(len, Self::get_bitmap_size());
        Block {
            data: hash,
            index_start: id_start,
        }
    }
}
#[cfg(test)]
mod test {
    use super::*;
    use std::fs::OpenOptions;
    use super::*;
    extern crate test;
    impl Serialize for u32 {
        fn size_of() -> usize {
            4
        }
        fn to_bytes(&self) -> Vec<u8> {
            u32::to_le_bytes(self.clone()).to_vec()
        }
        fn from_bytes(bytes: Vec<u8>) -> Self {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
    }
    #[test]
    fn build_block() {
        {
            let mut file = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open("block_test.db")
                .ok()
                .unwrap();
            file.set_len(Block::<u32>::get_block_size() as u64)
                .ok()
                .unwrap();
            let block = Block::<u32>::new(&mut file, 0);
            assert_eq!(block.len(), 0);
        }
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open("block_test.db")
            .ok()
            .unwrap();
        file.set_len(Block::<u32>::get_block_size() as u64)
            .ok()
            .unwrap();
        let block = Block::<u32>::load_from_disk(&mut file, 0);
        assert_eq!(block.len(), 0);
    }
    #[test]
    fn remove() {
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open("block_test_remove.db")
            .ok()
            .unwrap();
        file.set_len(Block::<u32>::get_block_size() as u64)
            .ok()
            .unwrap();
        let mut block = Block::<u32>::new(&mut file, 0);
        block.insert(&mut file, 0, 1);
        assert_eq!(block.get(&file, 0), &1);
        block.remove(&mut file,0);
        assert_eq!(block.len(), 0);
    }
}

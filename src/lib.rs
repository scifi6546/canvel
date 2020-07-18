use std::collections::HashMap;
use std::fs::OpenOptions;
use std::fs::File;
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::vec::Vec;
mod block;
use block::{Block,Serialize};

trait DbSized {
    const SIZE: u32;
}
impl Serialize for Row {
    fn to_bytes(&self) -> Vec<u8> {
        self.test_data.to_le_bytes().to_vec()
    }
    fn from_bytes(bytes: Vec<u8>)->Self {
        Row{
            test_data:u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
    }
    fn size_of()->usize{
        4
    }
}
#[derive(Clone)]
pub struct Row {
    test_data: u32,
}
pub enum DBError {
    IDNotInDatabase,
}
type ID = u32;
pub struct DB<'a> {
    file: File,
    data: RwLock<Vec<Mutex<Block<'a,Row>>>>,
}
//unsafe impl Sync for DB {}
impl<'a> DB<'a> {
    const MUTEX_SIZE: u32 = 4096;
    pub fn get_row(&self, id: ID) -> Result<Row, DBError> {
        if Self::contains_id(self.data.read().ok().unwrap(), id) {
            Ok(
                self.data.read().ok().unwrap()[(id.clone() / Self::MUTEX_SIZE) as usize]
                    .lock()
                    .ok()
                    .unwrap()[&id]
                    .clone(),
            )
        } else {
            Err(DBError::IDNotInDatabase)
        }
    }
    //creates new database with rows.
    //overwrites contents
    pub fn from_rows(data: &Vec<Row>,database_path:&str) -> (Self, Vec<ID>) {

        let mut file = OpenOptions::new().write(true).read(true).create(true).open(database_path).ok().unwrap();
        let mut blocks = vec![];
        //increasing block size by one
        let mut num_blocks = data.len()/Block::<Row>::BLOCK_SIZE as usize;
        if data.len()%Block::<Row>::BLOCK_SIZE as usize!=0{
            num_blocks+=1;
        }
        blocks.reserve(num_blocks);
        for i in 0..num_blocks{
            blocks.push(Mutex::new(Block::new(&mut file,i as u32*Block::<Row>::BLOCK_SIZE)));
        }
        let mut db = DB{
            file:file,
            data:RwLock::new(blocks)
        };
        let ids = data.iter().map(|r|{db.insert(r.clone())}).collect();
        return (db,ids);


    }
    //increases capacity of db by size
    fn grow<'b>(data: &'b mut RwLockWriteGuard<'b, Vec<Mutex<Block<'a,Row>>>>,size:usize){
        unimplemented!();
    }
    //Returns id and lock to current block to ensure that id is not taken
    fn get_free_id<'b>(
        data: &'b mut RwLockWriteGuard<'b, Vec<Mutex<Block<'a,Row>>>>,
    ) -> (
        ID,
        std::sync::MutexGuard<'b, Block<'a,Row>>,
    ) {
        {
            for i in 0..data.len() {
                //If there is a free id somewhere
                if data[i].lock().ok().unwrap().len() < Self::MUTEX_SIZE as usize {
                    //looking for free index
                    for j in (i as u32 * Self::MUTEX_SIZE)..(i as u32 + 1) * Self::MUTEX_SIZE {
                        if data[i].lock().ok().unwrap().contains(j) == false {
                            return (j, data[i].lock().ok().unwrap());
                        }
                    }
                    //should never reach this point
                    panic!("block with free element was actually full");
                }
            }
        }
        Self::grow(data,1);
        let lock = data[data.len() - 1].lock().ok().unwrap();
        let id = (data.len() as u32 - 1) * Self::MUTEX_SIZE;
        return (id, lock);
    }
    //inserts new row into database.
    pub fn insert(&mut self, row: Row)->ID {
        let mut write_lock = self.data.write().ok().unwrap();
        let (id, mut block) = Self::get_free_id(&mut write_lock);
        block.insert(id, row);
        return id;
    }
    /// Update row. Undefined if row is not already present
    fn update_inplace_row(
        data: RwLockReadGuard<
            '_,
            std::vec::Vec<std::sync::Mutex<Block<'a, Row>>>,
        >,
        id: ID,
        row: Row,
    ) {
        data[(id / Self::MUTEX_SIZE) as usize]
            .lock()
            .ok()
            .unwrap()
            .insert(id, row);
    }
    fn contains_id(data: RwLockReadGuard<'_, Vec<Mutex<Block<Row>>>>, id: ID) -> bool {
        if ((id / Self::MUTEX_SIZE) as usize) < data.len() {
            return data[(id / Self::MUTEX_SIZE) as usize]
                .lock()
                .ok()
                .unwrap()
                .contains(id);
        } else {
            return false;
        }
    }
    ///if id is not already present inserts new row. If id is present the row is overwritten.
    pub fn update_row(&mut self, id: ID, row: Row) -> Result<(), DBError> {
        if Self::contains_id(self.data.read().ok().unwrap(), id) {
            let read_lock = self.data.read().ok().unwrap();
            Self::update_inplace_row(read_lock, id, row);
            Ok(())
        } else {
            Err(DBError::IDNotInDatabase)
        }
    }
    ///Deletes row from db
    pub fn delete_row(&mut self, id: ID) -> Result<(), DBError> {
        if Self::contains_id(self.data.read().ok().unwrap(), id) {
            self.data.read().ok().unwrap()[(id / Self::MUTEX_SIZE) as usize]
                .lock()
                .unwrap()
                .remove(id);
            Ok(())
        } else {
            Err(DBError::IDNotInDatabase)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn make_rows(num: u32) -> Vec<Row> {
        (0..num).map(|i| Row { test_data: i }).collect()
    }
    #[test]
    fn make_db() {
        let db0 = DB::from_rows(&make_rows(0),"test1.db");
        let db1 = DB::from_rows(&make_rows(4096),"test2.db");
        let db2 = DB::from_rows(&make_rows(5000),"test3.db");
        assert_eq!(2 + 2, 4);
    }
    #[test]
    fn make_and_get() {
        let (db2, ids) = DB::from_rows(&make_rows(5000),"test4.db");
        for i in 0..5000 {
            assert_eq!(db2.get_row(i).ok().unwrap().test_data, i);
        }
    }
    #[test]
    fn update_row() {
        let (mut db2, ids) = DB::from_rows(&make_rows(5000),"test5.db");
        for id in ids.iter() {
            unsafe {
                db2.update_row(id.clone(), Row { test_data: 0 });
            }
        }
        for id in ids {
            assert_eq!(db2.get_row(id).ok().unwrap().test_data, 0);
        }
    }
}

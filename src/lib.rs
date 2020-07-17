use std::collections::HashMap;
use std::sync::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::vec::Vec;
mod block;

trait DbSized {
    const SIZE: u32;
}
trait Seralize {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(&mut self, bytes: Vec<u8>);
}
impl Seralize for Row {
    fn to_bytes(&self) -> Vec<u8> {
        self.test_data.to_le_bytes().to_vec()
    }
    fn from_bytes(&mut self, bytes: Vec<u8>) {
        self.test_data = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
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
pub struct DB {
    data: RwLock<Vec<Mutex<HashMap<ID, Row>>>>,
}
unsafe impl Sync for DB {}
impl DB {
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
    pub fn from_rows(data: &Vec<Row>) -> (Self, Vec<ID>) {
        let mut db = DB {
            data: RwLock::new(vec![]),
        };
        let mut current_id = 0;
        for row in data.iter() {
            //if at block boundry
            if current_id % Self::MUTEX_SIZE == 0 {
                db.data
                    .write()
                    .ok()
                    .unwrap()
                    .push(Mutex::new(HashMap::new()));
            }
            db.data.write().ok().unwrap()[(current_id / Self::MUTEX_SIZE) as usize]
                .lock()
                .ok()
                .unwrap()
                .insert(current_id, row.clone());
            current_id += 1;
        }
        return (db, (0..current_id).collect());
    }
    //Returns id and lock to current block to ensure that id is not taken
    fn get_free_id<'a, 'b>(
        data: &'b mut RwLockWriteGuard<'_, Vec<Mutex<HashMap<ID, Row>>>>,
    ) -> (
        ID,
        std::sync::MutexGuard<'b, std::collections::HashMap<u32, Row>>,
    ) {
        {
            for i in 0..data.len() {
                //If there is a free id somewhere
                if data[i].lock().ok().unwrap().len() < Self::MUTEX_SIZE as usize {
                    //looking for free index
                    for j in (i as u32 * Self::MUTEX_SIZE)..(i as u32 + 1) * Self::MUTEX_SIZE {
                        if data[i].lock().ok().unwrap().contains_key(&j) == false {
                            return (j, data[i].lock().ok().unwrap());
                        }
                    }
                    //should never reach this point
                    panic!("block with free element was actually full");
                }
            }
        }
        data.push(Mutex::new(HashMap::new()));
        let lock = data[data.len() - 1].lock().ok().unwrap();
        let id = (data.len() as u32 - 1) * Self::MUTEX_SIZE;
        return (id, lock);
    }
    //inserts new row into database.
    pub fn insert_row(&mut self, row: Row) {
        let mut write_lock = self.data.write().ok().unwrap();
        let (id, mut block) = Self::get_free_id(&mut write_lock);
        block.insert(id, row);
    }
    /// Update row. Undefined if row is not already present
    fn update_inplace_row(
        data: RwLockReadGuard<
            '_,
            std::vec::Vec<std::sync::Mutex<std::collections::HashMap<u32, Row>>>,
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
    fn contains_id(data: RwLockReadGuard<'_, Vec<Mutex<HashMap<u32, Row>>>>, id: ID) -> bool {
        if ((id / Self::MUTEX_SIZE) as usize) < data.len() {
            return data[(id / Self::MUTEX_SIZE) as usize]
                .lock()
                .ok()
                .unwrap()
                .contains_key(&id);
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
                .remove(&id);
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
        let db0 = DB::from_rows(&make_rows(0));
        let db1 = DB::from_rows(&make_rows(4096));
        let db2 = DB::from_rows(&make_rows(5000));
        assert_eq!(2 + 2, 4);
    }
    #[test]
    fn make_and_get() {
        let (db2, ids) = DB::from_rows(&make_rows(5000));
        for i in 0..5000 {
            assert_eq!(db2.get_row(i).ok().unwrap().test_data, i);
        }
    }
    #[test]
    fn update_row() {
        let (mut db2, ids) = DB::from_rows(&make_rows(5000));
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

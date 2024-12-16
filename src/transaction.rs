use crate::{
    data::ObjectId,
    error::*,
    object::{Object, Store},
    storage::StorageTransaction,
};

use std::{
    any::Any,
    cell::{Ref, RefCell, RefMut},
    collections::HashMap,
    marker::PhantomData,
    rc::Rc,
};

////////////////////////////////////////////////////////////////////////////////

type Repr = Rc<RefCell<CacheValue<dyn Store>>>;

pub struct Transaction<'a> {
    inner: Box<dyn StorageTransaction + 'a>,
    cache: RefCell<HashMap<ObjectId, Repr>>,
}

struct CacheValue<T: ?Sized> {
    state: ObjectState,
    obj: T,
}

impl<T> CacheValue<T> {
    fn new(obj: T) -> Self {
        CacheValue {
            state: ObjectState::Clean,
            obj,
        }
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(inner: Box<dyn StorageTransaction + 'a>) -> Self {
        Self {
            inner,
            cache: RefCell::default(),
        }
    }

    fn ensure_table_exists<T: Object>(&self) -> Result<()> {
        if !self.inner.table_exists(T::SCHEMA.table_name)? {
            self.inner.create_table(&T::SCHEMA)?;
        }
        Ok(())
    }

    pub fn create<T: Object>(&self, obj: T) -> Result<Tx<'_, T>> {
        self.ensure_table_exists::<T>()?;
        let id = self.inner.insert_row(&T::SCHEMA, &obj.to_row())?;
        let rc = Rc::new(RefCell::new(CacheValue::new(obj))) as Rc<RefCell<CacheValue<dyn Store>>>;
        self.cache.borrow_mut().insert(id, rc.clone());
        Ok(Tx::new(id, rc))
    }

    pub fn get<T: Object>(&self, id: ObjectId) -> Result<Tx<'_, T>> {
        let mut cache = self.cache.borrow_mut();
        let rc = match cache.entry(id) {
            std::collections::hash_map::Entry::Occupied(x) => {
                let e = x.get();
                match e.borrow().state {
                    ObjectState::Removed => {
                        return Err(Error::NotFound(Box::new(NotFoundError {
                            object_id: id,
                            type_name: T::SCHEMA.type_name,
                        })))
                    }
                    _ => e.clone(),
                }
            }
            std::collections::hash_map::Entry::Vacant(x) => {
                self.ensure_table_exists::<T>()?;
                let row = self.inner.select_row(id, &T::SCHEMA)?;
                let rc = Rc::new(RefCell::new(CacheValue::new(T::from_row(row))))
                    as Rc<RefCell<CacheValue<dyn Store>>>;
                x.insert(rc.clone());
                rc
            }
        };
        Ok(Tx::new(id, rc))
    }

    pub fn commit(self) -> Result<()> {
        for (id, v) in self.cache.borrow().iter() {
            let value = &v.borrow();
            let obj = &value.obj;
            match value.state {
                ObjectState::Clean => {}
                ObjectState::Modified => {
                    self.inner
                        .update_row(*id, obj.get_schema(), &obj.to_row())?
                }
                ObjectState::Removed => self.inner.delete_row(*id, obj.get_schema())?,
            };
        }
        self.inner.commit()
    }

    pub fn rollback(self) -> Result<()> {
        self.inner.rollback()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq)]
pub enum ObjectState {
    Clean,
    Modified,
    Removed,
}

#[derive(Clone)]
pub struct Tx<'a, T> {
    id: ObjectId,
    data: Rc<RefCell<CacheValue<dyn Store>>>,
    lifetime: PhantomData<&'a T>,
}

impl<'a, T: Any> Tx<'a, T> {
    pub fn id(&self) -> ObjectId {
        self.id
    }

    pub fn state(&self) -> ObjectState {
        self.data.borrow().state
    }

    pub fn borrow(&self) -> Ref<'_, T> {
        self.panic_if_removed();
        Ref::map(self.data.borrow(), |r| {
            r.obj.as_any().downcast_ref().unwrap()
        })
    }

    pub fn borrow_mut(&self) -> RefMut<'_, T> {
        self.panic_if_removed();
        let mut data = self.data.borrow_mut();
        data.state = ObjectState::Modified;
        RefMut::map(data, |r| r.obj.as_any_mut().downcast_mut().unwrap())
    }

    pub fn delete(self) {
        match self.data.try_borrow_mut() {
            Ok(mut data) => data.state = ObjectState::Removed,
            Err(_) => panic!("cannot delete a borrowed object"),
        }
    }

    fn panic_if_removed(&self) {
        assert!(
            self.data.borrow().state != ObjectState::Removed,
            "cannot borrow a removed object"
        );
    }
}

impl<'a, T> Tx<'a, T> {
    fn new(id: ObjectId, data: Rc<RefCell<CacheValue<dyn Store>>>) -> Self {
        Tx {
            id,
            data,
            lifetime: PhantomData,
        }
    }
}

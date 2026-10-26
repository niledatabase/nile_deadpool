#![cfg(feature = "managed")]

use std::{convert::Infallible, sync::Arc};

use deadpool::managed::{self, Metrics, Object, QueueMode, RecycleResult, Timeouts};

type Pool = managed::Pool<Manager>;

struct Manager;

impl managed::Manager for Manager {
    type Type = usize;
    type Error = Infallible;
    type LdbId = ();

    async fn create(&self, _: &(), _: Option<&str>) -> Result<usize, Infallible> {
        Ok(0)
    }

    async fn recycle(&self, _: &mut usize, _: &Metrics) -> RecycleResult<Infallible> {
        Ok(())
    }
}

fn generic_pool(max_size: usize) -> Pool {
    Pool::builder(Arc::new(Manager))
        .max_size(max_size)
        .queue_mode(QueueMode::None)
        .build()
        .unwrap()
}

#[tokio::test]
async fn returned_object_does_not_block_later_take() {
    let pool = generic_pool(1);

    drop(pool.get(&(), None).await.unwrap());
    let after_return = pool.status();

    let object = pool.get(&(), None).await.unwrap();
    let _ = Object::take(object);
    let after_take = pool.status();

    let object = pool
        .timeout_get(&Timeouts::wait_millis(0), &(), None)
        .await
        .unwrap();
    assert_eq!(after_return.size, 0);
    assert_eq!(after_return.available, 0);
    assert_eq!(after_take.size, 0);
    assert_eq!(after_take.available, 0);
    assert_eq!(pool.status().size, 1);
    drop(object);
    assert_eq!(pool.status().size, 0);
}

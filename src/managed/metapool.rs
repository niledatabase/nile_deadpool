use std::{fmt, marker::PhantomData, sync::Arc, time::Duration};

use moka::future::Cache;

use super::{
    BuildError, Manager, Object, Pool, PoolConfig, PoolError, QueueMode, Runtime, Timeouts,
};

/// Ratio of the maximum number of connection to use for the generic pool
const GENERIC_POOL_RATIO: f64 = 0.4;
/// Guesstimate of the average pool size for the meta pool
const META_POOL_AVG_SIZE: u64 = 5;

/// Meta pool.
///
/// Wraps some logic on top of a plain `Pool` to handle a limited number of per-ldbid pools, which
/// the expected reuse and recycling logic, and never reused connections for everything else.
///
/// Note that this does NOT guarantee that the provided maximum pool size will never be reached.
/// When a per-ldb pool is discarded the pooled connection still attached to it will continue to
/// live and won't be accounted for in the global maximum size.  They will eventually be destroyed
/// so the extra connection count is only temporary.
pub struct MetaPool<M: Manager, W: From<Object<M>> = Object<M>> {
    runtime: Option<Runtime>,
    manager: Arc<M>,
    timeouts: Timeouts,
    queue_mode: QueueMode,
    meta: Cache<M::LdbId, Arc<Pool<M, W>>>,
    generic: Pool<M, W>,
}

impl<M: Manager + 'static, W: From<Object<M>> + 'static> MetaPool<M, W> {
    /// Instantiates a builder for a new [`MetaPool`].
    ///
    /// This is the only way to create a [`MetaPool`] instance.
    pub fn builder(manager: M) -> MetaPoolBuilder<M, W> {
        MetaPoolBuilder::new(Arc::new(manager))
    }

    pub(crate) fn from_builder(builder: MetaPoolBuilder<M, W>) -> Result<Self, BuildError> {
        // Naive heuristics on how to use the given number of connections we were provided:
        //
        // - use 40% of the connections for the generic pool, ie one-shot connections that are
        //   immediately destroyed rather than pooled back and recycled
        // - use the rest of the connections for the meta pool.  We assume an average of 5
        //   connections per cached ldb.
        //
        // FIXME implement some dynamic pool size heuristics to allow popular pools to be bigger
        // than others, and eventually shrink down if needed.
        let generic_size = ((builder.config.max_size as f64) * GENERIC_POOL_RATIO) as usize;
        let num_cached_ldb = (builder.config.max_size - generic_size) as u64 / META_POOL_AVG_SIZE;

        // Generic pool that will never queue and recycle connections.
        let generic = MetaPool::create_one_pool(
            builder.manager.clone(),
            builder.config.timeouts.clone(),
            generic_size,
            QueueMode::None,
            builder.runtime,
        )?;

        // Create a new Timeouts for the meta pool.  We use the provded Timeouts, except the `wait`
        // part that we overload to never wait.  The meta pools are known to be rather small so
        // it's better to wait for a new connection rather than hoping that one of the meta pooled
        // connection will become available soon enough.
        let timeouts = Timeouts {
            wait: Some(Duration::ZERO),
            create: builder.config.timeouts.create,
            recycle: builder.config.timeouts.recycle,
        };

        Ok(MetaPool {
            runtime: builder.runtime,
            manager: builder.manager,
            timeouts: timeouts,
            queue_mode: builder.config.queue_mode,
            meta: Cache::new(num_cached_ldb),
            generic,
        })
    }

    /// Internal wrapper to create a Pool<M, W>.
    fn create_one_pool(
        manager: Arc<M>,
        timeouts: Timeouts,
        size: usize,
        queue_mode: QueueMode,
        runtime: Option<Runtime>,
    ) -> Result<Pool<M, W>, BuildError> {
        let mut builder = Pool::builder(manager).config(PoolConfig {
            max_size: size,
            timeouts,
            queue_mode,
        });

        if let Some(runtime) = runtime {
            builder = builder.runtime(runtime);
        }

        builder.build()
    }

    /// Returns [`Manager`] of this [`Pool`].
    #[must_use]
    pub fn manager(&self) -> &M {
        &self.manager
    }

    /// Meta version of `Pool::get()`.
    pub async fn get(&self, ldb_id: &M::LdbId) -> Result<W, PoolError<M::Error>> {
        self.timeout_get(&self.timeouts, ldb_id).await
    }

    /// Meta version of `Pool::timeout_get()`.
    pub async fn timeout_get(
        &self,
        timeouts: &Timeouts,
        ldb_id: &M::LdbId,
    ) -> Result<W, PoolError<M::Error>> {
        // Get a Pool from the metapool, or try to insert one if not found.
        let entry = self
            .meta
            .entry(ldb_id.clone())
            .or_insert_with(async {
                let pool = MetaPool::create_one_pool(
                    self.manager.clone(),
                    timeouts.clone(),
                    META_POOL_AVG_SIZE as usize,
                    self.queue_mode,
                    self.runtime,
                )
                // it's ok to `unwrap()` here.  If a runtime is missing `MetaPool::from_builder()`
                // would have errored out already.
                .unwrap();

                Arc::new(pool)
            })
            .await;

        // Since we rely on the TinyLFU policy there is no guarantee that a just inserted entry
        // passed the admission policy.  Since moka does not provide an API to know whether an
        // insert passed the admission policy, we need to check whether it were inserted or not,
        // and if not bail out and rely on the generic pool.
        // Note that the meta pools are kept rather small, so if we get a timeout here we will
        // ignore it and retry with the generic pool, using a longer timeout.
        let conn = if entry.is_fresh() {
            // Freshly inserted value, check if the key is in the cache.  We have to call
            // `run_pending_tasks` first as otherwise the entry might still be present in some
            // transient structure.
            self.meta.run_pending_tasks().await;

            if self.meta.contains_key(ldb_id) {
                // Key still present, we can use the entry.
                let pool = entry.into_value();
                Some(pool.timeout_get(timeouts, ldb_id).await)
            } else {
                // Ldb didn't pass addmission control, we will have to use the generic pool.
                None
            }
        } else {
            // Existing entry, just use the value.
            let pool = entry.into_value();
            Some(pool.timeout_get(timeouts, ldb_id).await)
        };

        // If we got a PoolError::Timeout when trying to acquire a connection from a meta pool,
        // ignore it and retry using the genric pool.  Everything other actual result (so
        // everything except an attempted ldb insertion that didn't pass admission control) is
        // returned as-is, whether it's an actual connection or an error.
        match conn {
            Some(res) => match res {
                Err(PoolError::Timeout(_)) => (),
                other => {
                    return other;
                }
            },
            _ => (),
        }

        // Could not get a connection from the meta pool, just use the generic pool.
        self.generic.get(ldb_id).await
    }
}

// Implemented manually to avoid unnecessary trait bound on `W` type parameter.
impl<M, W> fmt::Debug for MetaPool<M, W>
where
    M: fmt::Debug + Manager,
    M::Type: fmt::Debug,
    W: From<Object<M>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaPool")
            .field("manager", &self.manager)
            //.field("meta", &self.meta)
            .field("generic", &self.generic)
            .finish()
    }
}

/// Builder for [`MetaPool`]s.
///
/// Instances of this are created by calling the [`MetaPool::builder()`] method.
#[must_use = "builder does nothing itself, use `.build()` to build it"]
pub struct MetaPoolBuilder<M, W = Object<M>>
where
    M: Manager,
    W: From<Object<M>>,
{
    pub(crate) manager: Arc<M>,
    pub(crate) config: PoolConfig,
    pub(crate) runtime: Option<Runtime>,
    _wrapper: PhantomData<fn() -> W>,
}

// Implemented manually to avoid unnecessary trait bound on `W` type parameter.
impl<M, W> fmt::Debug for MetaPoolBuilder<M, W>
where
    M: fmt::Debug + Manager,
    W: From<Object<M>>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetaPoolBuilder")
            .field("manager", &self.manager)
            .field("config", &self.config)
            .field("runtime", &self.runtime)
            .field("_wrapper", &self._wrapper)
            .finish()
    }
}

impl<M, W> MetaPoolBuilder<M, W>
where
    M: Manager + 'static,
    W: From<Object<M>> + 'static,
{
    pub(crate) fn new(manager: Arc<M>) -> Self {
        Self {
            manager,
            config: PoolConfig::default(),
            runtime: None,
            _wrapper: PhantomData,
        }
    }

    /// Builds the [`Pool`].
    ///
    /// # Errors
    ///
    /// See [`BuildError`] for details.
    pub fn build(self) -> Result<MetaPool<M, W>, BuildError> {
        // Return an error if a timeout is configured without runtime.
        let t = &self.config.timeouts;
        if (t.wait.is_some() || t.create.is_some() || t.recycle.is_some()) && self.runtime.is_none()
        {
            return Err(BuildError::NoRuntimeSpecified);
        }
        MetaPool::from_builder(self)
    }

    /// Sets a [`PoolConfig`] to build the ['MetaPool']'s [`Pool`] with.
    pub fn config(mut self, value: PoolConfig) -> Self {
        self.config = value;
        self
    }

    /// Sets the [`PoolConfig::max_size`].
    pub fn max_size(mut self, value: usize) -> Self {
        self.config.max_size = value;
        self
    }

    /// Sets the [`PoolConfig::timeouts`].
    pub fn timeouts(mut self, value: Timeouts) -> Self {
        self.config.timeouts = value;
        self
    }

    /// Sets the [`Timeouts::wait`] value of the [`PoolConfig::timeouts`].
    pub fn wait_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.wait = value;
        self
    }

    /// Sets the [`Timeouts::create`] value of the [`PoolConfig::timeouts`].
    pub fn create_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.create = value;
        self
    }

    /// Sets the [`Timeouts::recycle`] value of the [`PoolConfig::timeouts`].
    pub fn recycle_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.timeouts.recycle = value;
        self
    }

    /// Sets the [`Runtime`].
    ///
    /// # Important
    ///
    /// The [`Runtime`] is optional. Most [`Pool`]s don't need a
    /// [`Runtime`]. If want to utilize timeouts, however a [`Runtime`] must be
    /// specified as you will otherwise get a [`PoolError::NoRuntimeSpecified`]
    /// when trying to use [`Pool::timeout_get()`].
    ///
    /// [`MetaPoolBuilder::build()`] will fail with a
    /// [`BuildError::NoRuntimeSpecified`] if you try to build a
    /// [`MetaPool`] with timeouts and no [`Runtime`] specified.
    ///
    /// [`PoolError::NoRuntimeSpecified`]: super::PoolError::NoRuntimeSpecified
    pub fn runtime(mut self, value: Runtime) -> Self {
        self.runtime = Some(value);
        self
    }
}

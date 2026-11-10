use async_trait::async_trait;
use loom_contracts::*;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use wasmtime::{
    Config, Engine, Store, StoreLimits, StoreLimitsBuilder,
    component::{Component, Linker},
};

mod bindings {
    wasmtime::component::bindgen!({ path: "../../wit", world: "guest" });
}
use bindings::Guest;

struct Host {
    limits: StoreLimits,
}

/// Fresh isolated instance per turn; compiled components are cached by immutable digest.
pub struct WasmEngine {
    engine: Engine,
    objects: Arc<dyn ObjectStore>,
    cache: Arc<Mutex<HashMap<String, Component>>>,
    fuel: u64,
    memory_bytes: usize,
}
fn invalid(e: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("{e:#}"))
}
impl WasmEngine {
    pub fn new(objects: Arc<dyn ObjectStore>) -> Result<Self> {
        let mut config = Config::new();
        config.wasm_component_model(true).consume_fuel(true);
        Ok(Self {
            engine: Engine::new(&config).map_err(invalid)?,
            objects,
            cache: Arc::default(),
            fuel: 20_000_000,
            memory_bytes: 32 * 1024 * 1024,
        })
    }
    async fn invoke(
        &self,
        module: &ObjectRef,
        name: &str,
        state: Option<Bytes>,
        input: Bytes,
    ) -> Result<Bytes> {
        const MAX_PAYLOAD: usize = 4 * 1024 * 1024;
        if input.len() > MAX_PAYLOAD || state.as_ref().is_some_and(|x| x.len() > MAX_PAYLOAD) {
            return Err(invalid("guest input exceeds 4 MiB"));
        }
        let cached = self.cache.lock().unwrap().get(&module.digest).cloned();
        let bytes = if cached.is_none() {
            Some(self.objects.get(module).await?)
        } else {
            None
        };
        let engine = self.engine.clone();
        let cache = self.cache.clone();
        let digest = module.digest.clone();
        let name = name.to_owned();
        let fuel = self.fuel;
        let memory = self.memory_bytes;
        tokio::task::spawn_blocking(move || {
            let component = match cached {
                Some(c) => c,
                None => {
                    let bytes = bytes.unwrap();
                    // Only compile Wasm bytes. Never deserialize untrusted native artifacts.
                    if bytes.len() > 16 * 1024 * 1024 {
                        return Err(invalid("module exceeds 16 MiB"));
                    }
                    let c = Component::new(&engine, &bytes).map_err(invalid)?;
                    cache.lock().unwrap().insert(digest, c.clone());
                    c
                }
            };
            let limits = StoreLimitsBuilder::new()
                .memory_size(memory)
                .instances(32)
                .memories(4)
                .tables(4)
                .build();
            let mut store = Store::new(&engine, Host { limits });
            store.limiter(|h| &mut h.limits);
            store.set_fuel(fuel).map_err(invalid)?;
            let linker = Linker::<Host>::new(&engine);
            let guest = Guest::instantiate(&mut store, &component, &linker).map_err(invalid)?;
            let result = match state {
                Some(state) => guest.call_handle(&mut store, &name, &state, &input),
                None => guest.call_task(&mut store, &name, &input),
            }
            .map_err(invalid)?
            .map_err(invalid)?;
            if result.len() > MAX_PAYLOAD {
                return Err(invalid("guest output exceeds 4 MiB"));
            }
            Ok(result)
        })
        .await
        .map_err(invalid)?
    }
}
#[async_trait]
impl GuestEngine for WasmEngine {
    async fn task(&self, module: &ObjectRef, name: &str, input: Bytes) -> Result<Bytes> {
        self.invoke(module, name, None, input).await
    }
    async fn actor(
        &self,
        module: &ObjectRef,
        name: &str,
        state: Bytes,
        message: Bytes,
    ) -> Result<Decision> {
        let bytes = self.invoke(module, name, Some(state), message).await?;
        serde_json::from_slice(&bytes).map_err(invalid)
    }
}

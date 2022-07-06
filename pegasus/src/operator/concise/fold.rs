use std::fmt::Debug;

use crate::api::function::FnResult;
use crate::api::{Fold, Unary};
use crate::communication::output::OutputProxy;
use crate::stream::{Single, SingleItem, Stream};
use crate::tag::tools::map::TidyTagMap;
use crate::{BuildJobError, Data};

impl<D: Data> Fold<D> for Stream<D> {
    fn fold<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        let aggregated = self.aggregate();
        fold_impl("fold", aggregated, init, factory)
    }

    fn fold_partition<B, F, C>(self, init: B, factory: C) -> Result<SingleItem<B>, BuildJobError>
    where
        B: Clone + Send + Sync + Debug + 'static,
        F: FnMut(B, D) -> FnResult<B> + Send + 'static,
        C: Fn() -> F + Send + 'static,
    {
        fold_impl("fold_partition", self, init, factory)
    }
}

fn fold_impl<D, B, F, Fo>(
    name: &str, stream: Stream<D>, init: B, factory: F,
) -> Result<SingleItem<B>, BuildJobError>
where
    D: Data,
    B: Clone + Send + Sync + Debug + 'static,
    Fo: FnMut(B, D) -> FnResult<B> + Send + 'static, // the fold function defined by user;
    F: Fn() -> Fo + Send + 'static,
{
    let s = stream.unary(name, |info| {
        let mut table = TidyTagMap::<(B, Fo)>::new(info.scope_level);
        move |input, output| {
            input.for_each_batch(|batch| {
                if !batch.is_empty() {
                    let (mut accum, mut f) = table
                        .remove(&batch.tag)
                        .unwrap_or((init.clone(), factory()));

                    for d in batch.drain() {
                        accum = f(accum, d)?;
                    }
                    table.insert(batch.tag.clone(), (accum, f));
                }

                if let Some(end) = batch.take_end() {
                    if let Some((accum, _)) = table.remove(&batch.tag) {
                        let mut session = output.new_session(&batch.tag)?;
                        session.give_last(Single(accum), end)?;
                    } else {
                        // decide if it need to output a default value when upstream is empty;
                        // but only one default value should be output;
                        let worker = crate::worker_id::get_current_worker().index;
                        if end.contains_source(worker)
                        {
                            let mut session = output.new_session(&batch.tag)?;
                            session.give_last(Single(init.clone()), end)?
                        } else {
                            output.notify_end(end)?;
                        }
                    }
                }
                Ok(())
            })
        }
    })?;
    Ok(SingleItem::new(s))
}
